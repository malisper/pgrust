# Audit: backend-utils-adt-mcxtfuncs

C source: `src/backend/utils/adt/mcxtfuncs.c` (PostgreSQL 18.3, 305 lines).
c2rust reference: none under `../pgrust/c2rust-runs` for this file; src-idiomatic
reference: `../pgrust/src-idiomatic/crates/backend-utils-adt-mcxtfuncs`.

Crate splits mcxtfuncs.c out of the `backend-utils-adt-sqlhelpers` bundle row
(same as dbsize.c/xid8funcs.c earlier).

## Function-by-function

Every function in mcxtfuncs.c is enumerated.

### `int_list_to_array` (static, C:46-61)
C: `palloc(length * sizeof(Datum))`, fills `Int32GetDatum(i)` for each list
element in order, then `construct_array_builtin(datum_array, length, INT4OID)`,
returns `PointerGetDatum(result_array)`.

Port: `int_list_to_array(list: &[i32]) -> Vec<i32>` returns the path values in
list order. The `int4[]` array Datum assembly (`construct_array_builtin`,
INT4OID) is folded into the `tuplestore_putvalues` seam — the provider side that
also does `CStringGetTextDatum`/`Int*GetDatum` and feeds the SRF tuplestore. The
in-crate logic preserves the exact element order. PARITY (value layer in-crate;
Datum/array assembly is the fmgr-boundary seam, project-wide deferral).

### `PutMemoryContextsStatsTupleStore` (static, C:67-175)
- `path = NIL`; `Assert(MemoryContextIsValid(context))` — port reads the node via
  `context_node` seam (carries the validity surface as `PgResult`).
- Ancestor loop `for (cur = context; cur != NULL; cur = cur->parent)`:
  `hash_search(HASH_FIND)`, `if (!found) elog(ERROR, "hash table corrupted")`
  (C:98) via `errmsg_internal` at line 98; `path = lcons_int(entry->context_id,
  path)` → `path.insert(0, id)` (prepend). PARITY.
- `(*context->methods->stats)(context, NULL, NULL, &stat, true)` → `context_stats`
  seam returning `MemoryContextCounters`. PARITY.
- `name = context->name; ident = context->ident`. PARITY.
- dynahash relabel: `if (ident && strcmp(name,"dynahash")==0){name=ident;ident=NULL;}`
  → `if ident.is_some() && name == Some(b"dynahash") { name = ident.take() }`.
  PARITY.
- `values[0]`: name text or `nulls[0]`; raw server-encoding bytes carried (no
  lossy UTF-8) so `CStringGetTextDatum` is byte-faithful. PARITY.
- `values[1]`: ident clip — `idlen = strlen(ident)`; if `idlen >=
  MEMORY_CONTEXT_IDENT_DISPLAY_SIZE (1024)` then `pg_mbcliplen(ident, idlen,
  1023)`; `memcpy(clipped, ident, idlen)`. Port uses `pg_mbcliplen` seam +
  byte slice `ident[..idlen]`. Constant 1024 verified. PARITY.
- type switch (C:142-160): T_AllocSetContext→"AllocSet", T_GenerationContext→
  "Generation", T_SlabContext→"Slab", T_BumpContext→"Bump", default→"???".
  Modeled as `MemoryContextType` (resolved from `context->type` on the seam
  provider side). Strings verified. PARITY.
- `values[2]=type`, `values[3]=Int32GetDatum(list_length(path))` (level),
  `values[4]=int_list_to_array(path)`, `values[5]=totalspace`,
  `values[6]=nblocks`, `values[7]=freespace`, `values[8]=freechunks`,
  `values[9]=totalspace-freespace`. All 10 columns + `used_bytes` subtraction
  match (`wrapping_sub` mirrors C's int64 wrap). PARITY.
- `tuplestore_putvalues(...)` → seam; `list_free(path)` → Vec drop. PARITY.

### `pg_get_backend_memory_contexts` (SQL SRF, C:181-249)
- `hash_create(..., 256, HASH_ELEM|HASH_BLOBS|HASH_CONTEXT)` → insertion-ordered
  Vec keyed by handle identity (HASH_BLOBS = identity key). PARITY.
- `InitMaterializedSRF(fcinfo, 0)` — fmgr SRF boundary; performed by the
  (deferred) fmgr entry point, not the core. See deferral note.
- `contexts = list_make1(TopMemoryContext)` → `top_memory_context` seam.
- breadth-first `foreach_ptr` with `lappend` during iteration → index-based
  `while idx < len` loop appending children (C semantics: list grows while
  iterated). PARITY.
- `context_id = 1` for TopMemoryContext, `entry->context_id = context_id++`,
  `Assert(!found)` → `debug_assert!` of absence. PARITY.
- children walk `for (c = cur->firstchild; c; c = c->nextchild) lappend`. PARITY.
- `hash_destroy` → Vec drop; `return (Datum) 0`.
- The breadth-first algorithm is `pg_get_backend_memory_contexts_core() ->
  PgResult<()>`; the `PG_FUNCTION_ARGS` wrapper `pg_get_backend_memory_contexts`
  is a loud `panic!` (project-wide fmgr/Datum SRF deferral), NOT a stub of the
  logic, NOT a todo!. The InitMaterializedSRF / `(Datum) 0` / fcinfo plumbing is
  the deferred surface.

### `pg_log_backend_memory_contexts` (SQL function, C:264-305)
- `pid = PG_GETARG_INT32(0)` — taken as the `pid: i32` parameter (fmgr arg
  extraction is the deferral; the function body is fully ported).
- `proc = BackendPidGetProc(pid); if (proc==NULL) proc = AuxiliaryPidGetProc(pid)`
  + `GetNumberFromPGProc(proc)` → folded into `pid_get_proc` seam returning
  `Option<McxtSignalTarget{proc_number}>`. PARITY.
- `if (proc==NULL) ereport(WARNING, errmsg("PID %d is not a PostgreSQL server
  process", pid)); PG_RETURN_BOOL(false)` (C:293) → `errmsg` (translatable),
  line 293, `return Ok(false)`. PARITY.
- `procNumber = GetNumberFromPGProc(proc)` → `proc.proc_number`. PARITY.
- `if (SendProcSignal(pid, PROCSIG_LOG_MEMORY_CONTEXT, procNumber) < 0)
  ereport(WARNING, errmsg("could not send signal to process %d: %m", pid));
  PG_RETURN_BOOL(false)` (C:302). Reuses the real
  `backend-storage-ipc-procsignal-seams::send_proc_signal` (returns the kill
  result; `< 0` check). `%m` expands against current errno in the error
  subsystem. PROCSIG_LOG_MEMORY_CONTEXT verified = 5 in types-storage. PARITY.
- `PG_RETURN_BOOL(true)` → `Ok(true)`. PARITY.

## Constants verified
- `MEMORY_CONTEXT_IDENT_DISPLAY_SIZE = 1024` (C:24).
- `PG_GET_BACKEND_MEMORY_CONTEXTS_COLS = 10` (C:72).
- `PROCSIG_LOG_MEMORY_CONTEXT = 5` (types_storage::ProcSignalReason).
- type-string table {AllocSet, Generation, Slab, Bump, ???}.
- ereport line numbers 98 / 293 / 302 mirror the C.

## Seams
Owns `backend-utils-adt-mcxtfuncs-seams` with 5 OUTWARD seams whose real owners
are the still-unported mcxt.c remainder / funcapi.c / procarray.c+proc.c:
`top_memory_context`, `context_node`, `context_stats`, `tuplestore_putvalues`,
`pid_get_proc`. Reuses `send_proc_signal` (procsignal, already declared) and
`pg_mbcliplen` (mbutils, already declared). The `MemoryContext` cursor is the
opaque `MemoryContextRef` — inherited opacity for an unported owner (the C type
is `MemoryContextData *` used only as a dynahash key + walk cursor), not invented.
Owns no INWARD seams → `init_seams()` is empty and the crate installs nothing, so
it is not added to `seams-init::init_all` (the recurrence guard exempts it). Both
seams-init guards pass: every declared seam is recognized as outward (the owner
`::call`s each), and no install regression.

## Deviations
- The two `PG_FUNCTION_ARGS` SQL entry points are loud `panic!`s
  (`fmgr/Datum-layer deferral: ...`), per the project-wide fmgr/Datum SRF
  deferral. The full algorithmic cores are exposed and tested. No `todo!`/
  `unimplemented!`.
- `int_list_to_array`'s array Datum construction and the per-column
  `CStringGetTextDatum`/`Int*GetDatum` assembly live on the provider side of the
  `tuplestore_putvalues` seam (the SRF result-Datum boundary), not in-crate.

## Tests
8 unit tests (breadth-first ids+path, unknown type "???", dynahash relabel,
non-UTF-8 ident byte-faithful, oversize ident clip to 1023, log-warns-on-missing,
log-sends-signal, log-warns-on-signal-failure). All pass. `cargo check
--workspace`, `cargo test -p seams-init`, `cargo test -p no-todo-guard` green.

VERDICT: PASS.
