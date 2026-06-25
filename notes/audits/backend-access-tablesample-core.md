# Audit: backend-access-tablesample-core

Unit: `backend-access-tablesample-core`
C sources: `src/backend/access/tablesample/{system.c, bernoulli.c, tablesample.c}` (PostgreSQL 18.3)
Result: **PASS**

## Scope

The SYSTEM (block-level) and BERNOULLI (tuple-level) TABLESAMPLE method
handlers plus the `tablesample.c` `GetTsmRoutine` support helper. This crate is
the owner of the tablesample-method layer below the (landed) `nodeSamplescan`
executor node and the planner's `set_tablesample_rel_size`.

## Per-function parity

### system.c (5 fns)

| C function | Port | Notes |
|---|---|---|
| `tsm_system_handler` | `tsm_system_handler()` | `makeNode(TsmRoutine)` -> `T_TsmRoutine` (440). `list_make1_oid(FLOAT4OID)` -> `vec![700]`. `repeatable_across_{queries,scans}=true`. Installs all callbacks except `EndSampleScan=NULL`. |
| `system_samplescangetsamplesize` | `system_samplescangetsamplesize()` | `linitial(paramexprs)` -> `paramexprs[0]`; `estimate_expression_value`; `IsA(Const) && !constisnull` -> `Expr::Const(c) if !c.constisnull`; `DatumGetFloat4(constvalue)`; range/NaN check then `/100.0f` else `0.1f`; `*pages = clamp_row_est(pages*samplefract)` (multiply in f32 then widen, matching C usual arithmetic conversions); `*tuples = clamp_row_est(tuples*samplefract)`. |
| `system_initsamplescan` | `system_initsamplescan()` | `palloc0(sizeof(SystemSamplerData))` -> `tsm_state = Opaque(Box::new(SystemSamplerData::default()))`. `eflags` unused (as in C). |
| `system_beginsamplescan` | `system_beginsamplescan()` | `DatumGetFloat4(params[0])`; `percent<0||>100||isnan` -> ereport(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT); `dcutoff = rint(((double)PG_UINT32_MAX+1)*percent/100)` via `round_ties_even()` (rint = ties-to-even, NOT Rust `round`); `cutoff/seed/nextblock=0/lt=Invalid`; `use_bulkread=(percent>=1)`, `use_pagemode=true`. |
| `system_nextsampleblock` | `system_nextsampleblock()` | `hashinput[1]=seed`; `for(;nextblock<nblocks;nextblock++){hashinput[0]=nextblock; hash=hash_any(hashinput,8); if(hash<cutoff)break;}`; found -> `nextblock=nextblock+1; return nextblock`; else `nextblock=0; return InvalidBlockNumber`. |
| `system_nextsampletuple` | `system_nextsampletuple()` | `tupoffset=lt`; Invalid->First else +1; `>maxoffset->Invalid`; `lt=tupoffset`; return. |

### bernoulli.c (5 fns)

| C function | Port | Notes |
|---|---|---|
| `tsm_bernoulli_handler` | `tsm_bernoulli_handler()` | As SYSTEM but `NextSampleBlock=NULL` (tuple-level) and `EndSampleScan=NULL`. |
| `bernoulli_samplescangetsamplesize` | `bernoulli_samplescangetsamplesize()` | Same fold/Const path as SYSTEM, but `*pages = baserel->pages` (visits all pages) and `*tuples = clamp_row_est(tuples*samplefract)`. |
| `bernoulli_initsamplescan` | `bernoulli_initsamplescan()` | `palloc0(sizeof(BernoulliSamplerData))` -> Opaque box. |
| `bernoulli_beginsamplescan` | `bernoulli_beginsamplescan()` | Same validation + `rint` cutoff; `cutoff/seed/lt=Invalid`; `use_bulkread=true`, `use_pagemode=(percent>=25)`. |
| `bernoulli_nextsampletuple` | `bernoulli_nextsampletuple()` | `tupoffset=lt`; Invalid->First else +1; `hashinput[0]=blockno; hashinput[2]=seed`; `for(;tupoffset<=maxoffset;tupoffset++){hashinput[1]=tupoffset; hash=hash_any(hashinput,12); if(hash<cutoff)break;}`; `>maxoffset->Invalid`; `lt=tupoffset`; return. |

### tablesample.c (1 fn)

| C function | Port | Notes |
|---|---|---|
| `GetTsmRoutine` | `GetTsmRoutine()` | `OidFunctionCall1(tsmhandler, NULL)` fmgr dispatch -> OID map (3314 system, 3313 bernoulli from pg_proc.dat); `routine==NULL || !IsA(routine,TsmRoutine)` -> exact `"tablesample handler function %u did not return a TsmRoutine struct"` error (verified against the routine `type_ == T_TsmRoutine`); routine allocated in caller `mcx` (C result lifetime). |

## Constants / PRNG / math verification

- `T_TsmRoutine = 440` — verified against `src/backend/nodes/nodetags.h:457`.
- `FLOAT4OID = 700`, `FirstOffsetNumber = 1`, `InvalidOffsetNumber = 0`,
  `InvalidBlockNumber = 0xFFFFFFFF`, `PG_UINT32_MAX = 0xFFFFFFFF` — verified.
- Handler OIDs `3313` (bernoulli), `3314` (system) — verified against
  `pg_proc.dat`.
- Cutoff: `rint(((double)PG_UINT32_MAX+1)*percent/100)`. `rint` rounds to nearest
  with ties-to-even (default FP mode); ported with `f64::round_ties_even()` (NOT
  `f64::round`, which rounds ties away from zero). Test `cutoff_matches_c_formula`
  checks 0/50/100% endpoints (0, 2^31, 2^32).
- `hash_any((const unsigned char *)hashinput, sizeof(hashinput))` is reproduced
  with `common_hashfn::hash_bytes` over the native-endian byte serialization of
  the `uint32[2]`/`uint32[3]` arrays — the exact bytes the C `(const unsigned
  char *)` cast exposes (machine-independent for these inputs, as the C comments
  note). Test `hash_inputs_are_native_bytes` confirms the byte assembly.
- Note: PG 18.3 SYSTEM/BERNOULLI use `hash_any` (not `sampler_random_fract` /
  the old `pg_prng`-based path that the task brief mentioned); the brief's
  PRNG-fract API is not present in these C files for this version.

## Architecture / seam wiring

- `TsmRoutine` callbacks are real Rust fn pointers (mirror C `tsm->X = fn`),
  operating in place on `&mut SampleScanState<'mcx>` — the table-AM/index-AM
  vtable convention. The C `void *node->tsm_state` is the owned `Opaque(Box<dyn
  Any>)`; callbacks downcast to `SystemSamplerData`/`BernoulliSamplerData`.
- **Contract reconcile (keystone)**: `types-samplescan`'s callback fn-ptr
  typedefs were `fn(&mut SampleScanState<'static>, ..)`, which the `<'mcx>`
  dispatch seams could not call. Widened them to mcx-free higher-ranked
  `for<'mcx> fn(&mut SampleScanState<'mcx>, ..)`. No callers of those pointers
  existed (verified by grep), so the change is additive; all consumers
  (`nodeSamplescan`, `allpaths`, `pgrust-pg-ffi-fgram`) still compile.
- `init_seams()` (wired into `seams-init::init_all`) installs the
  `nodeSamplescan-seams`: `get_tsm_routine_oid` (= `GetTsmRoutine`), and the
  vtable dispatch wrappers `tsm_has_init_sample_scan` / `tsm_init_sample_scan` /
  `tsm_begin_sample_scan` / `tsm_has_next_sample_block` / `tsm_has_end_sample_scan`
  / `tsm_end_sample_scan` (read `node.tsmroutine`'s pointers and call them).

## mirror-PG-and-panic / deferrals

- `tsm->NextSampleBlock` / `tsm->NextSampleTuple` are invoked by the table AM
  (`table_scan_sample_next_{block,tuple}`, heapam_handler.c, unported) — they
  live in the routine vtable for that owner; not dispatched here.
- The planner-facing `SampleScanGetSampleSize` is reached by the planner through
  the `backend-optimizer-path-allpaths` `tsm_get_sample_size` /
  `tsm_repeatable_across_scans` / `tsm_is_parallel_safe` seams, which require
  RTE -> `tablesample` -> `{tsmhandler,args}` navigation owned by the unported
  planner-entry (`Query<'mcx>`) crate + `backend-optimizer-rte-seams`. Those
  seams are left as loud panics (mirror-PG-and-panic). The faithful folding
  bodies are the pub fns `{system,bernoulli}_samplescangetsamplesize`, ready for
  that owner. The vtable `SampleScanGetSampleSize` slot is an ABI-matching shim
  taking C's default `else` branch, since the landed fn-ptr type carries neither
  an `Mcx` nor the walkable args the fold path needs.
- The `percent` validation error in `*_beginsamplescan` surfaces as a panic
  carrying the exact `PgError` message/SQLSTATE, because the `BeginSampleScan`
  callback ABI has no error channel (the loud-failure convention).

## Stubs

No `todo!`/`unimplemented!`/own-logic stubs. Every method body is the full C
logic; the only non-installed surfaces are seam-and-panic into the correct
(unported) owners.

## Tests

5 unit tests pass: handler shapes, cutoff formula endpoints, the tuple-offset
walk, the hash-input byte assembly, and `GetTsmRoutine` resolve/reject.
