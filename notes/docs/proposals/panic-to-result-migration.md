# panic → Result error-model migration

Status: **Phase 1 landed** (coexistence foundation + one-crate POC). Phases 2+
are the follow-on campaign seeded below.

## Goal

Eliminate every *recoverable* panic from the tree. Today a SQL-level error (a
builtin raising `ereport(ERROR)`) is delivered as `panic_any(PgError)` and caught
by `catch_unwind` at the fmgr dispatch boundary, then re-surfaced as
`Err(PgError)`. The end state: every error path returns `Result` threaded with
`?`, so the `catch_unwind` boundaries can be dropped and the build can run
`panic=abort` — a panic then means a genuine bug (abort), never a recoverable SQL
error.

Wins: **−~17MB of unwind tables** in the release binary, ability to use
`panic=abort`, and elimination of the abort-in-abort / non-unwind-safe-cleanup
crash class (the #1 hang/crash source this session — ~6 fixed). Cost: ~+0.2ns at
the shallow fmgr boundary (frequency-weighted negligible).

This MUST be incremental: `main` stays green at every commit (the whole
regression fleet branches from main). Bodies migrate one crate at a time.

---

## Surface map (measured)

### The `PGFunction` fn-ptr type and where it is named/stored

- **Canonical def**: `crates/types-fmgr/src/fmgr.rs`
  `pub type PGFunction = Option<fn(&mut FunctionCallInfoBaseData) -> Datum>;`
  (re-exported from `types-fmgr/src/lib.rs`). The bare-`Datum` return is the
  irreducible C-ABI return slot — it cannot carry a `Result`, which is the entire
  reason errors currently travel by panic.
- A second `PGFunction` alias exists in `crates/types-nodes/src/execexpr.rs`
  (`for<'mcx> fn(&mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx>`) and a raw
  FFI one in `crates/pgrust-pg-ffi-fgram/src/fmgr.rs` (the c2rust shim, not on the
  ported dispatch path).
- **Stored in**: `BuiltinFunction.func: PGFunction`
  (`crates/types-fmgr/src/resolution.rs`) — one row of the builtin table — and
  `FmgrInfo.fn_addr: PGFunction` (`crates/types-fmgr/src/fmgr.rs`). The builtin
  registry (`crates/backend-utils-fmgr-core`) holds the rows by Oid.
- **The `_v1` wrappers**: ported builtins are written as `fc_<name>(&mut fcinfo)
  -> Datum` adapters (the hand-port of `PG_FUNCTION_INFO_V1` bodies). They read
  args off the frame, call the value core, and on error call a crate-local
  `raise(e) -> !` = `std::panic::panic_any(e)`.

### Builtin bodies (the panicking `-> Datum` functions)

**1,778 builtin bodies across 93 crates.** Top crates by count:

| crate | bodies |
|---|---|
| backend-utils-adt-datetime | 190 |
| backend-utils-adt-misc2 | 134 |
| backend-utils-adt-float | 120 |
| backend-utils-adt-geo-ops | 106 |
| backend-utils-adt-varlena | 103 |
| backend-utils-adt-int8 | 82 |
| backend-utils-adt-acl | 50 |
| backend-utils-adt-int | 45 |
| backend-utils-adt-varbit | 42 |
| backend-utils-adt-regexp | 41 |
| backend-utils-adt-arrayfuncs | 40 |
| backend-utils-adt-network | 37 |
| backend-utils-adt-cash | 36 |
| backend-utils-adt-multirangetypes | 36 |
| backend-utils-adt-numeric | 34 |
| backend-utils-adt-xid | 33 |
| backend-utils-adt-varchar | 33 |
| … (76 more, tapering to leaf crates of 7–20) | … |

Registration: each crate's `init_seams()` calls a local
`register_<unit>_builtins()` which calls
`backend_utils_fmgr_core::register_builtins([...])` with `BuiltinFunction { foid,
name, nargs, strict, retset, func: Some(fc_x) }` rows. **216** `BuiltinFunction {
… }` literal construction sites across the tree — almost all are a single local
`fn builtin(...)` helper per crate, so a per-crate migration touches ~1 helper +
the registration list.

### `catch_unwind` sites (18 total)

| category | count | sites |
|---|---|---|
| (a) fmgr boundary | 1 | `backend-utils-fmgr-core/src/lib.rs` `invoke_pgfunction` |
| (b) tcop / portal | 5 | tcop main loop (×2), pquery portal body, portalmem resowner guard, guc hook-init |
| (c) other production | 3 | plpgsql EXCEPTION (`PG_TRY/PG_CATCH`), gram parser abort sentinel, `types-rel` relation-close-in-`Drop` |
| (d) test harness | 9 | `#[cfg(test)]` only — out of scope |

Load-bearing analysis of the 9 production sites:

- **(a) fmgr boundary** — the target. Removable once *all* builtins are Native.
- **(b) tcop main-loop / portal / resowner** — the SQL error-recovery spine. These
  catch the *panic that a legacy builtin raises*. As crates migrate, the error
  arrives as `Err` instead; once every builtin is Native and the body-level panics
  are gone, these collapse to plain `Result` propagation (the loop already has a
  `PgResult` path). The guc hook-init catch is boot robustness, droppable with the
  GUC hook migration.
- **(c) plpgsql EXCEPTION + gram parser** — these are *intentional* language/FFI
  boundaries (PL/pgSQL `EXCEPTION` literally is `PG_TRY/PG_CATCH`; the parser
  returns NULL on a syntax-error sentinel). They migrate to `Result` last, with
  their own subsystems.
- **(c) `types-rel` `Drop` close guard** — a genuine panic-during-panic safety
  valve (close-on-unwind must not double-panic → `abort()`). This one **stays even
  under `panic=abort`**; it is not a recovery boundary.

### "not ported" recoverable panic stubs (~43)

Honest `panic!("… not ported …")` / seam-miss stubs (NOT `todo!`/`unimplemented!`).
Concentrations: `backend-utils-sort-storage` (14 sharedtuplestore seam stubs),
`backend-utils-adt-misc2` (9 window-fn + flinfo introspection),
`backend-replication-walsender` (7), `backend-access-transam-xlog` (4),
`backend-utils-adt-ruleutils` (2), plus ~6 singletons. These eventually become
`return Err(PgError::feature_not_supported(…))` — but ONLY where the C path is a
real recoverable error; a genuinely-unported body stays a stub (do not weaken it).
A `no-todo-guard` crate test forbids introducing new `todo!`/`unimplemented!`.

### Executor angle (already Result-threaded)

The interpreter is **already** Result-native end to end:
`ExecInterpExpr -> PgResult<(Datum,bool)>`
(`backend-executor-execExprInterp/src/interp_loop.rs`); its `EEOP_FUNCEXPR` step
calls `eval_scalar::exec_func_step(...)?`, which calls
`function_call_invoke_datum::call(...)?` (the fmgr seam) and propagates with `?`.
That seam bottoms out in fmgr-core's `function_call_invoke_datum_seam` →
`function_call_invoke_with_expr`. **The only remaining panic hop in the entire SQL
execution path is inside `function_call_invoke_with_expr`, at the single builtin
dispatch arm.** That is exactly the chokepoint Phase 1 makes Result-native.

---

## The coexistence mechanism (Phase 1, landed)

Purely additive. No change to the `BuiltinFunction` struct, no change to any of
the 216 construction sites, no change to any unmigrated crate.

1. **New type** (`types-fmgr/src/fmgr.rs`):
   ```rust
   pub type PgFnNative =
       fn(&mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum>;
   ```
   Identical calling convention to `PGFunction`, but the error channel is the
   return value (`Err(PgError)`) instead of `panic_any(PgError)`.

2. **Parallel registry** (`backend-utils-fmgr-core`): a second thread_local
   `NATIVE: HashMap<Oid, PgFnNative>` overlaying the legacy `REGISTRY`.
   - `register_builtin_native(entry: BuiltinFunction, native: PgFnNative)` registers
     the ordinary metadata row into `REGISTRY` (so `fmgr_isbuiltin`, by-name lookup,
     strict/retset, the gap-guard — every existing reader — work unchanged) AND
     records `native` in `NATIVE` keyed by `entry.foid`. A migrated crate sets the
     row's `func` to `None` (the legacy callable is unused).
   - `register_builtins_native(...)` bulk form; `native_builtin(foid)` lookup;
     `clear_builtins()` clears both.

3. **Dispatch** (`function_call_invoke_with_expr`'s single builtin arm now calls a
   new `invoke_builtin(b, fcinfo)`):
   ```rust
   fn invoke_builtin(b, fcinfo) -> PgResult<Datum> {
       if let Some(native) = native_builtin(b.foid) {
           let _current = push_current_fcinfo(fcinfo); // same snapshot as legacy
           return native(fcinfo);                      // direct, no catch_unwind
       }
       invoke_pgfunction(&b.func, fcinfo)              // legacy catch_unwind bridge
   }
   ```
   `invoke_pgfunction` STILL returns `PgResult<Datum>` (it always did), so the
   caller is unchanged. With zero crates migrated, `NATIVE` is empty and every call
   takes the legacy branch — byte-for-byte prior behavior.

The `DirectFunctionCall{1..9}` family (internal C-to-C direct calls that pass a
bare `&PGFunction`, not a `BuiltinFunction`) stays on the legacy bridge for now;
those are not SQL-visible and rarely error. They migrate in a later phase if at
all.

---

## The one-crate POC (Phase 1, landed)

`backend-utils-adt-int8` migrated to Native (canonical because of the
`SELECT 1/0` → `division_by_zero` and `(-9223372036854775808)/(-1)` →
`bigint_out_of_range` hard errors). Each `fc_*` body is now
`fn(&mut fcinfo) -> PgResult<Datum>`: `ret_x(v)` → `Ok(ret_x(v))`, the
`raise(e)`/`match …Err(e) => raise(e)` paths → `return Err(e)` / `?`. Registered
via `register_builtins_native`. Verified:

- `cargo build --bin postgres` green.
- `SELECT 1::int8 + 2::int8` etc. return correct results.
- `SELECT (1::int8) / (0::int8)` produces a clean `ERROR: division by zero` to the
  client via `Err` (no panic, no `catch_unwind`) and **the backend session
  survives** (next query in the same session succeeds).

---

## Phased plan for the rest of the campaign

The work is **~93 crates** (1,778 bodies). Each crate migration is mechanical and
independently mergeable on top of the foundation.

- **Phase 2 — leaf adt crates (batch, ~50 crates).** Migrate the scalar/leaf adt
  crates bottom-up: int, int8 (done), oid, bool, char, float, numeric, varchar,
  bpchar, xid, cash, uuid, network, etc. Each: rewrite the `fc_*` bodies to
  `-> PgResult<Datum>`, swap `raise`/`panic_any` for `Err`/`?`, register via
  `register_builtins_native`. Suggested batching: ~8–10 crates per lane, grouped by
  family (numeric family, string family, datetime family, geo family). Gate per
  crate: build + a query that exercises one error path survives the session.
  Soft-error (`escontext`) paths are unchanged — they already return, not panic.

- **Phase 3 — heavier adt + access/catalog SQL functions (~30 crates).** datetime
  (190), misc2 (134 — note the unported window-fn stubs stay stubs), geo-ops (106),
  varlena (103), arrayfuncs, jsonb/json/jsonfuncs, regexp, ruleutils, acl. Same
  mechanism; larger bodies.

- **Phase 4 — the SRF / aggregate / window frames + `DirectFunctionCall` family.**
  Set-returning and aggregate transition functions; decide whether the internal
  direct-call family migrates. **Assessed + partially landed — see "Phase 4 status"
  below.** Aggregate transfn/finalfn (18 builtins) MIGRATED to native; SRFs are a
  separate executor-frame ABI (own refactor, deferred); `plpgsql_call_handler` needs
  the PL-executor `Result` campaign (Phase 5).

- **Phase 5 — subsystem boundaries.** plpgsql `EXCEPTION` → `Result`-based dispatch
  (its catch becomes a `match` on the returned `Err`), gram parser sentinel →
  `Result`, the `~43` "not ported" stubs → `Err(feature_not_supported)` where the C
  path is recoverable.

- **Phase 6 — drop `catch_unwind` + `panic=abort`.** Once no builtin/body raises a
  recoverable panic: remove the fmgr-boundary catch (a) and the tcop/portal catches
  (b); keep ONLY the genuine FFI/panic-during-panic guards (`types-rel` `Drop`
  close, any thread boundary). Set `panic=abort` in the release profile. Re-measure
  the binary (target: −~17MB unwind tables) and confirm the smoke + regress suites.

## Phase 4 status (landed in part) — SRF / aggregate / `plpgsql_call_handler`

Phase 4 was assessed category-by-category. The three deferred categories do **not**
share one ABI, so they do not share one migration:

### (1) Aggregate transition/final functions — MIGRATED ✓

`array_agg` (arrayfuncs, 2 builtins), `json_agg`/`json_object_agg` family (json, 8),
`jsonb_agg`/`jsonb_object_agg` family (jsonb, 8) — **18 builtins** — were ordinary
by-OID fmgr `fn(&mut fcinfo) -> Datum` panic bodies (`raise(e)=panic_any` / `ok(r)`
helpers, dispatched through `invoke_builtin` → `invoke_pgfunction`'s `catch_unwind`;
the nodeAgg `invoke_transfn`/`invoke_finalfn` seams bottom out at that same
chokepoint). This is exactly the Phase 2 scalar shape, and
`backend-utils-adt-numeric::agg_fmgr` was already native (the reference). All 18
migrated to `-> PgResult<Datum>` + `register_builtins_native` (the helper bodies
`arg_value`/`elem_datum`/`*_impl` + the internal-state ctors threaded `?` too).
Verified on a booted cluster: correct values for int/text array_agg and the json[b]
aggregates; the error paths (`json_object_agg` NULL key, `jsonb_object_agg_unique`
duplicate key) deliver a clean `ERROR:` via `Err` with **zero panics** in the server
log and the session survives. These entries now have **no recoverable panic**.

### (2) Set-returning functions — NOT the Phase-2 mechanism (different ABI)

The Phase-2 lever (`register_builtins_native`, by-OID `NATIVE` overlay,
`PgFnNative = fn(&mut types_fmgr::fcinfo) -> PgResult<Datum>`) is **structurally
inapplicable** to SRFs. SRFs do not dispatch through the by-OID fmgr builtin
registry at all: the fmgr frame's `resultinfo` is tag-only (the WONTFIX dual-home),
so it can never carry a live `ReturnSetInfo`. They register into a **separate
executor-frame table** (`backend-executor-execSRF::register_srf`) whose callable is
`types_nodes::execexpr::PGFunction = for<'mcx> fn(&mut fcinfo<'mcx>) -> Datum<'mcx>`
(the lifetime-carrying executor `Datum`, NOT the `usize`-Datum fmgr frame), and
dispatch is `srf_invoke_by_oid` → `f(fcinfo)` directly. ~10 of the named SRFs
(`regexp_matches`/`_split_to_table`, `json[b]_each`/`_array_elements`/`_object_keys`,
`jsonb_path_query`/`_tz`, `tsvector_unnest`, `pg_snapshot_xip`) are already ported
into that table (materialize-mode via `InitMaterializedSRF` or value-per-call via
`MultiFuncCall`); two (`ts_stat1/2`, `pg_get_backend_memory_contexts`) are unported
loud stubs. **They still PANIC on a SQL error** — the adapter boundary uses
`.unwrap_or_else(|e| panic_any(e))`, and there is **no `catch_unwind` in execSRF**,
so a bad-regex / bad-jsonpath error propagates as a panic up to the tcop/portal
catch (category (b)): the session survives, but the panic is not eliminated at the
SRF boundary. Faithfully eliminating it is a **separate, self-contained refactor**:
change the `register_srf` registry's `PGFunction` type to return
`PgResult<Datum<'mcx>>`, thread `?` through `srf_invoke_by_oid`/`dispatch_user_setof`
and the two execSRF call sites, and convert the ~120 `panic_any`/`panic!` adapter
sites across the 27 `backend-executor-execSRF/src/*.rs` files to `Err`/`?`. This is
an execSRF-owned lane, not a by-OID fmgr crate migration, and is deferred.

### (3) `plpgsql_call_handler` — genuinely can't be native yet (still panics-to-recover)

`plpgsql_call_handler(fcinfo) -> Datum` (NOT `PgResult`) calls the PL executor
(`plpgsql_exec_function -> FunctionResult`, `plpgsql_exec_trigger -> Datum`,
`compile_for_call -> PLpgSQL_function` — **all by-value, none `PgResult`**), which
raises every SQL error via `seam::propagate` = `panic_any(PgError)` deep inside
frames that are not `?`-threaded. It stays on the legacy `func: Some(...)` panic
bridge (caught at `invoke_pgfunction`'s `catch_unwind`); the session recovers, but
**it still panics on every SQL error raised inside a PL/pgSQL body**. (Its siblings
`plpgsql_inline_handler`/`plpgsql_validator` are already Result-native — their cores
return `PgResult` — so only `call_handler` remains.) Making it native requires
threading `PgResult` through the entire PL executor call tree (re-signing
`plpgsql_exec_function`/`_trigger`/`_event_trigger` and their callees from by-value
to `PgResult`), which is the Phase 5 plpgsql-subsystem campaign — it also subsumes
the plpgsql `EXCEPTION` (`PG_TRY/PG_CATCH`) boundary. Deferred to Phase 5.

### Remaining recoverable-panic surface after Phase 4 (Phase 5/6 scope)

For "no recoverable panic survives" (the Phase 6 precondition), what is left:

- **SRF adapters** — ~120 `panic_any` sites across execSRF (category (2) above);
  the registry-ABI refactor clears them.
- **`plpgsql_call_handler` + the PL executor** — every in-body SQL error still
  panics through the non-`?`-threaded executor (category (3)); the plpgsql `Result`
  campaign clears it, and folds in the `EXCEPTION` `PG_TRY/PG_CATCH` boundary.
- **The ~43 "not ported" stubs** — `panic!("… not ported …")` seam-miss stubs
  (sharedtuplestore, window-fn introspection, walsender, xlog, ruleutils, +
  singletons); these become `Err(feature_not_supported)` only where the C path is a
  real recoverable error, else stay honest stubs.
- **The gram parser sentinel** — the syntax-error NULL-return boundary → `Result`.
- **The tcop/portal `catch_unwind`s (category (b))** — these are the *recovery*
  boundaries that catch all of the above; they collapse to plain `Result`
  propagation only once every producer above is panic-free.
- **Genuine non-recovery guards kept under `panic=abort`** — the `types-rel` `Drop`
  close-on-unwind guard and any panic-during-panic / thread-boundary guard.

So after Phase 4 the recoverable-panic frontier is dominated by exactly two
self-contained campaigns (the execSRF registry-ABI refactor and the plpgsql-executor
`Result` threading) plus the stub/parser cleanups — no remaining *by-OID fmgr scalar
or aggregate* builtin raises a recoverable panic.

### Walls / cautions

- The `DirectFunctionCall` family passes `&PGFunction`, not a `BuiltinFunction`, so
  it cannot consult `NATIVE` by Oid as-is. If those need migrating, either thread
  the Oid or have callers hold the native fn directly. Out of scope for leaf phases.
- The two non-canonical `PGFunction` aliases (`types-nodes`, `pgrust-pg-ffi-fgram`)
  are separate; the ported dispatch only uses the `types-fmgr` one.
- Soft-error (`escontext`) input functions already return via the sink — do not
  convert those to hard `Err`; preserve the soft/hard distinction faithfully.
- Keep the `types-rel` `Drop` close guard (and any other panic-during-panic guard)
  even under `panic=abort`.
</content>
</invoke>
