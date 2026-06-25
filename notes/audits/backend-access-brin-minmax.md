# Audit: backend-access-brin-minmax (brin_minmax.c)

Independent re-derivation from `../pgrust/postgres-18.3/src/backend/access/brin/brin_minmax.c`
and `../pgrust/c2rust-runs/backend-access-brin-brin-minmax`.

## Function inventory (C → port)

| C function (brin_minmax.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `brin_minmax_opcinfo` (:33) | `brin_minmax_opcinfo` | MATCH | oi_nstored=2, oi_regular_nulls=true, palloc0 MinmaxOpaque = `MinmaxOpaque::default()` (Cells zeroed to InvalidOid), `oi_typcache[0]=oi_typcache[1]=lookup_type_cache(typoid,0)`. Returns PgBox into mcx (allocating ⇒ Mcx+PgResult). |
| `brin_minmax_add_value` (:63) | `brin_minmax_add_value` | MATCH | `Assert(!isnull)`=debug_assert. allnulls branch stores datumCopy to both [0],[1] and clears allnulls, returns true. Else LESS cmp(newval, v[0]) ⇒ overwrite v[0]; GREATER cmp(newval, v[1]) ⇒ overwrite v[1]; updated OR-accumulated. subtype = attr->atttypid (atttypid), colloid = PG_GET_COLLATION. attr read from bd_tupdesc. pfree(by-ref) = canonical-Datum overwrite (no explicit free in the byte model). |
| `brin_minmax_consistent` (:136) | `brin_minmax_consistent` | MATCH | `Assert(PG_NARGS()==3)` fixed by 3-arg call site (consistent_is_multi→false). `Assert(!bv_allnulls)`=debug_assert. Strategy switch: LT/LE → cmp(v[0],value); EQ → LE(v[0],value) then (if true) GE(v[1],value) two-call short-circuit; GE/GT → cmp(v[1],value); default → elog(ERROR,"invalid strategy number %d") XX000. subtype=key.sk_subtype, value=key.sk_argument. Returns the matches bool (C PG_RETURN_DATUM of a bool-op result; caller does DatumGetBool). |
| `brin_minmax_union` (:207) | `brin_minmax_union` | MATCH | attno-eq + !allnulls asserts = debug_assert. LESS(b[0],a[0]) ⇒ a[0]=datumCopy(b[0]); GREATER(b[1],a[1]) ⇒ a[1]=datumCopy(b[1]). subtype=attr->atttypid. col_b untouched. |
| `minmax_get_strategy_procinfo` (:260, static) | `minmax_get_strategy_procinfo` | MATCH | strategynum 1..=5 assert. subtype-change ⇒ clear all 5 slots to InvalidOid + set cached_subtype. If slot==InvalidOid: opfamily=bd_index->rd_opfamily[attno-1], atttypid=bd_tupdesc attr; SearchSysCache4(AMOPSTRATEGY)+amopopr = `get_opfamily_member(opfamily, atttypid, subtype, strategynum)`; missing ⇒ elog(ERROR,"missing operator %d(%u,%u) in opfamily %u") XX000; RegProcedureIsValid assert = debug_assert; fmgr_info_cxt(get_opcode(oprid),...) cached as `get_opcode(oprid)` OID. |

No MISSING / PARTIAL / DIVERGES. C has exactly these 5 functions (verified against c2rust rendering); all present.

## Carrier decision (S1, reused by S2-S4)

C `MinmaxOpaque { Oid cached_subtype; FmgrInfo strategy_procinfos[BTMax]; }` lives in the
`palloc0` tail of `BrinOpcInfo` (`void *oi_opaque`). Reconciled onto the real model:
`types_brin::OpaqueOpcInfo` placeholder `[u8]` → typed enum (one variant per built-in
opclass); `MinmaxOpaque` caches the resolved comparison-fn `Oid` per BTree strategy in
`Cell`s. Justification: the repo's `function_call2_coll` re-resolves by OID, so the OID is
the whole callable identity (no FmgrInfo body needs caching); `Cell` interior mutability
lets the BRIN AM's lazy fill mutate through the immutable `&BrinDesc` the dispatch seams
pass — matching C's mutation of `bdesc->bd_info[]->oi_opaque` through a pointer. Not
invented opacity: it is the real C `oi_opaque` struct, trimmed and re-keyed to the repo's
by-OID fmgr model.

## Seam audit

Owned seam crate: `backend-access-brin-entry-seams` maps to `brin.c`/`brin_bloom.c`/
`brin_minmax_multi.c` per CATALOG — i.e. the BRIN-AM/opclass dispatch surface. The minmax
unit OWNS+installs the 6 opclass-support-dispatch seams (single installer per CLAUDE.md):
`brin_opcinfo`, `brin_addvalue`, `brin_union`, `brin_consistent_is_multi`,
`brin_consistent_single`, `brin_consistent_multi` — all `set()` in `init_seams()`, wired
into `seams-init::init_all()`. The 7th seam `brin_serialize` is NOT owned (minmax registers
no serialize callback; it is owned by the bloom/minmax-multi stage). Each dispatcher is thin:
`index_getprocinfo(index, keyno+1, PROCNUM).fn_oid` then a match on the built-in
`F_BRIN_MINMAX_*` OIDs (3383-3386, verified against pg_proc.dat) to the in-crate bodies;
non-minmax built-ins seam-and-panic (their stage unported) — a loud panic on an unported
callee, not absent logic.

Outward seam calls — all real cross-crate cycles, thin marshal+delegate:
- `typcache::lookup_type_cache` (oi_typcache),
- `lsyscache::get_opfamily_member` + `get_opcode` (amop strategy resolution),
- `fmgr::function_call2_coll` (the comparison FunctionCall2Coll),
- `scalar::datum_copy` (datumCopy),
- `indexam::index_getprocinfo` (support-proc OID resolution).

No logic in any seam path or in `init_seams()`.

## Design conformance

- Allocating fns (`brin_minmax_opcinfo`, `add_value`, `union`) take `Mcx` and return
  `PgResult` (OOM surface). MATCH.
- No invented opacity (typed `OpaqueOpcInfo` enum = the real C struct; OID = real fn OID).
- No shared statics, no ambient-global seams, no locks across `?`, no registry side table.
- `elog(ERROR)` → `Err(PgError)` with XX000 (ERRCODE_INTERNAL_ERROR), matching C
  `errcode` default for elog. No unledgered divergence markers.
- Constants: F_BRIN_MINMAX_{OPCINFO,ADD_VALUE,CONSISTENT,UNION}=3383..3386 (pg_proc.dat),
  BRIN_PROCNUM_*=1..4 (brin_internal.h), BT strategy 1..5 (stratnum.h) — verified.

## Verdict: PASS

All 5 functions MATCH; seams owned/installed/thin; design rules satisfied; 8 unit tests
(opcinfo shape, add_value all-nulls + both-end extension, consistent equal two-call
short-circuit, invalid strategy error, cache subtype invalidation, union, missing
operator). Note: by-ref-type fmgr dispatch rides the repo-wide canonical-Datum word lane
(same inherited limitation as nbtree/execExprInterp), not a divergence introduced here.
