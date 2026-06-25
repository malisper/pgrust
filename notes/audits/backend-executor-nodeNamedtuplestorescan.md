# Audit: backend-executor-nodeNamedtuplestorescan

Independent function-by-function audit of `port/backend-executor-nodeNamedtuplestorescan`
against the original C and the c2rust translation.

- C source: `src/backend/executor/nodeNamedtuplestorescan.c`
- c2rust run: `../pgrust/c2rust-runs/backend-executor-nodeNamedtuplestorescan/src/nodeNamedtuplestorescan.rs`
- Port: `crates/backend-executor-nodeNamedtuplestorescan/src/lib.rs`

## 1. Function inventory

The C file defines exactly 5 functions. c2rust confirms 5 (`NamedTuplestoreScanNext`,
`NamedTuplestoreScanRecheck`, `ExecNamedTuplestoreScan`, `ExecInitNamedTuplestoreScan`,
`ExecReScanNamedTuplestoreScan`). No `#if`-gated alternates outside the build config.
There is intentionally no `ExecEndNamedTuplestoreScan` (T_NamedTuplestoreScanState is in
execProcnode's "no cleanup" group).

## 2. Per-function comparison

| C function (loc) | Port location | Verdict | Notes |
|---|---|---|---|
| `NamedTuplestoreScanNext` (static, l.30) | `NamedTuplestoreScanNext` (lib.rs:72) | MATCH | `slot = ss_ScanTupleSlot; tuplestore_select_read_pointer(relation, readptr); tuplestore_gettupleslot(relation, true, false, slot)`. Forward-only Assert is an executor invariant (no runtime branch in C); omitted as in C. `(void)` return-discard of gettupleslot becomes the `Ok(bool)` end-of-scan flag — slot emptiness reported via the boolean, behavior identical. |
| `NamedTuplestoreScanRecheck` (static, l.51) | `NamedTuplestoreScanRecheck` (lib.rs:98) | MATCH | Returns `true` unconditionally. |
| `ExecNamedTuplestoreScan` (static, l.65) | `ExecNamedTuplestoreScan` (lib.rs:117) + `exec_named_tuplestore_scan_node` (lib.rs:132) | MATCH | C `castNode(NamedTuplestoreScanState, pstate)` is the pointer cast in the ExecProcNode wrapper (`exec_named_tuplestore_scan_node`, panicking on tag mismatch like castNode's Assert); `ExecScan(&ss, NextMtd, RecheckMtd)` -> `exec_scan_namedtuplestore::call(node, estate, Next, Recheck)`. Access/recheck mtds passed through faithfully. |
| `ExecInitNamedTuplestoreScan` (l.80) | `ExecInitNamedTuplestoreScan` (lib.rs:155) | MATCH | makeNode -> alloc_in (fallible, PgResult on OOM). plan/state/ExecProcNode set. `get_ENR` not-found -> `elog(ERROR, "executor could not find named tuplestore \"%s\"")` mapped to `PgError::error` (default XX000 = ERRCODE_INTERNAL_ERROR, matching elog ERROR). `enr->reldata` non-owning alias via NonNull (Opacity inherited: real Tuplestorestate, no invented handle). `ENRMetadataGetTupDesc` called directly (queryenvironment ported). `tuplestore_alloc_read_pointer(EXEC_FLAG_REWIND)`, select+rescan, ExecAssignExprContext, ExecInitScanTupleSlot(TTSOpsMinimalTuple), ExecInitResultTypeTL, ExecAssignScanProjectionInfo, ExecInitQual(scan.plan.qual) — all in C order. The two Asserts (no outer/inner plan, unsupported eflags) -> debug_assert. tupdesc is deep-cloned where C shares a pointer; behavior-preserving (descriptor content identical). |
| `ExecReScanNamedTuplestoreScan` (l.155) | `ExecReScanNamedTuplestoreScan` (lib.rs:294) | MATCH | `if ps_ResultTupleSlot: ExecClearTuple`; `ExecScanReScan(&ss)`; `tuplestore_select_read_pointer(ts, readptr); tuplestore_rescan(ts)`. Order and predicate preserved. |

No own-logic stubs, no `todo!`/`unimplemented!`, no deferred/unsupported-error escapes.

## 3. Seam audit

**Owned inward seams: none.** This unit's only c_source is nodeNamedtuplestorescan.c;
no `crates/backend-executor-nodeNamedtuplestorescan-seams` exists. `init_seams()` is empty
and correct (the crate declares no inward seams). It is still wired into
`seams-init::init_all()` (lib.rs:77), satisfying the recurrence guard.

Outward seam calls (all thin marshal + delegate into genuinely-unported owners):
- execScan (unported owner): `exec_scan_namedtuplestore`, `exec_assign_scan_projection_info`,
  `exec_scan_rescan_ss`. The `exec_scan_namedtuplestore` seam carries the node's own
  Next/Recheck mtds; the EPQ/qual/project loop is execScan-owned — correct (logic is NOT
  pulled out of this crate, it never belonged here).
- backend-utils-sort-storage (tuplestore.c, unported owner): `tuplestore_select_read_pointer`,
  `tuplestore_gettupleslot`, `tuplestore_alloc_read_pointer`, `tuplestore_rescan`.
- execTuples (unported owner): `exec_init_scan_tuple_slot`, `exec_clear_tuple`.
- execUtils (unported owner): `exec_assign_expr_context`, `exec_init_result_type_tl`.
- execExpr (unported owner): `exec_init_qual`.

All are real dependency-cycle delegations to named unported owners; each is argument
conversion + one call + result conversion, no branching/computation in any seam path.

## 3b. Design conformance

- Opacity inherited, not introduced: `relation` is a `NonNull` alias of the real
  `Tuplestorestate`, not an invented handle (types.md 6-7 ok).
- Allocating entry point returns `PgResult` and allocates in `estate.es_query_cxt` Mcx — ok.
- No shared statics, no ambient-global seams, no locks across `?`, no registry side tables.
- es_queryEnv trimmed off EState; QueryEnvironment threaded explicitly into ExecInit — a
  documented, behavior-preserving signature change (not a divergence in logic).

## 4. Gates

- `cargo check --workspace`: PASS
- `cargo test -p backend-executor-nodeNamedtuplestorescan`: PASS (0 tests)
- `cargo test -p seams-init`: PASS (recurrence_guard both checks green:
  `every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`)

## Verdict: PASS

All 5 functions MATCH. No MISSING/PARTIAL/DIVERGES. Seam ownership correct (no inward
seams owned; empty `init_seams()` wired). Zero seam findings, zero design findings.
CATALOG.tsv row set to `audited`.
