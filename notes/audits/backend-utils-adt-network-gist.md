# Audit: backend-utils-adt-network-gist

C source: `src/backend/utils/adt/network_gist.c` (PostgreSQL 18.3, 810 lines).
Port: `crates/backend-utils-adt-network-gist/src/lib.rs`.
Seam crate: `crates/backend-utils-adt-network-gist-seams`.
Vocabulary types (`GistInetKey`, `GistInetSplitVec`, `inet_struct` codecs):
`crates/types-network/src/lib.rs`.
By-OID dispatch wiring: `crates/backend-access-gist-proc/src/lib.rs`.

No c2rust run exists for this unit (`c2rust-runs/` has only
`backend-utils-adt-network`); audited against the C directly plus the
src-idiomatic base.

## Function inventory and verdicts

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `inet_gist_consistent` (114) | `inet_gist_consistent` | MATCH | Checks 0–5 ported branch-for-branch; `*recheck=false`; strategy fall-through → `elog(ERROR,"unknown strategy for inet GiST")` via `PgError::error` (default SQLSTATE = ERRCODE_INTERNAL_ERROR, == elog ERROR). `GIST_LEAF(ent)` surfaced as `is_leaf`. |
| 2 | `calc_inet_union_params` (344, static) | `calc_inet_union_params` | MATCH | Inclusive `m..=n`; minfamily/maxfamily/minbits/commonbits + `bitncommon` reduce; force 0 on family mismatch. |
| 3 | `calc_inet_union_params_indexed` (406, static) | `calc_inet_union_params_indexed` | MATCH | Same, indexed via `offsets[0..noffsets]`. |
| 4 | `build_inet_union_key` (471, static) | `build_inet_union_key` | MATCH | `palloc0` → zeroed struct; `(commonbits+7)/8` byte copy; partial-byte mask `~(0xFF >> (commonbits%8))`. Varlena header is the fmgr boundary's concern. |
| 5 | `inet_gist_union` (504) | `inet_gist_union` | MATCH | `calc_inet_union_params(ent,0,n-1)`; family→0 on mismatch; addr from `ent[0]`. |
| 6 | `inet_gist_compress` (541) | `inet_gist_compress` | MATCH | Leaf-only conversion; family set first, then `commonbits=gk_ip_maxbits(r)`, `gk_ip_addrsize` copy. NULL key → `None` (C `(Datum)0`). Inner passthrough + `palloc(GISTENTRY)` done in the dispatch wrapper. |
| 7 | `inet_gist_fetch` (589) | `inet_gist_fetch` | MATCH | `family`, `bits=minbits`, `ip_addrsize(dst)` copy. `SET_INET_VARSIZE` is the fmgr boundary. |
| 8 | `inet_gist_penalty` (619) | `inet_gist_penalty` | MATCH | family≠ → 4; minbits degrade → 3; else `1/commonbits` or 2; `bitncommon(min(commonbits))`. |
| 9 | `inet_gist_picksplit` (662) | `inet_gist_picksplit` | MATCH | Split-by-family / split-on-bit / 50-50 fallback; fixed-size slot arrays + explicit counts mirror the palloc'd `left[spl_nleft++]`/`right[]`; per-side union recomputed from scratch reading `left[0]`/`right[0]` (always-allocated slot, as in C). `palloc` → fallible `try_reserve` → `PgResult`. |
| 10 | `inet_gist_same` (796) | `inet_gist_same` | MATCH | family && minbits && commonbits && `memcmp(addr, gk_ip_addrsize(left))`. |

Macros (`gk_ip_*`, `ip_*`, `ip_family_maxbits`, `OffsetNumberNext`,
`FirstOffsetNumber`, `SET_GK_VARSIZE`/`DatumGetInetKeyP`) ported as inline
helpers / `to_datum_bytes`/`from_datum_bytes` codecs. Strategy constants
verified against `access/stratnum.h` (OVERLAPS 3, EQ 18, NE 19, LT 20, LE 21,
GT 22, GE 23, SUB 24, SUBEQ 25, SUP 26, SUPEQ 27).

## Seam audit

This unit owns `backend-utils-adt-network-gist-seams` (covers `network_gist.c`).
It declares the seven typed inet GiST support-procedure bodies; the owning crate
installs **all seven** in `init_seams()` (consistent/union/compress/fetch/
penalty/picksplit/same) with thin non-capturing marshal closures, and
`seams-init::init_all()` calls `backend_utils_adt_network_gist::init_seams()`.
`recurrence_guard` (both directions) passes.

Consumers: `backend-access-gist-proc` (the single installer of the GiST by-OID
dispatch) routes the inet proc OIDs (3553/3554/3555/3557/3558/3559/3573, verified
against `pg_proc.dat`) to these seams, marshaling `GISTENTRY.key` / query Datum
via the `GistInetKey`/`inet_struct` byte codecs. The dispatch arms are thin
marshal+delegate (decode Datum → call seam → encode result), no logic.

The seam boundary is justified: `backend-access-gist-proc` ↔
`backend-utils-adt-network-gist` would otherwise be a direct edge, but the GiST
core dispatches opclass procs by OID through a seam layer (mirroring BRIN/SP-GiST),
so the inet bodies install into a seam exactly like the box/point bodies install
into `backend-access-gist-dispatch-seams`.

## Design conformance

- No invented opacity: `GistInetKey`/`inet_struct` are real field-accurate
  structs in `types-network`; Datum carried as `Datum::ByRef` bytes (repo
  convention, same as `BOX`/`Point`).
- Allocating paths fallible: `inet_gist_picksplit` (`try_reserve`) and the
  dispatch `inet_key_datum`/fetch (`mcx::slice_in`) return `PgResult`; the
  by-value methods (consistent/penalty/same/compress/fetch/union return Copy
  payloads) don't allocate in the caller's context.
- No shared statics, no ambient-global seams, no locks, no registry side tables,
  no `todo!`/`unimplemented!`. The only error path (`unknown strategy`) is
  `Err(PgError)` with the matching SQLSTATE.

## Verdict: PASS

Every function MATCH; all seven owned seams installed and wired; dispatch arms
are marshal-only; design rules satisfied.
