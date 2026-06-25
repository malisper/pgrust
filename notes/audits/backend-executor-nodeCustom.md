# Audit: backend-executor-nodeCustom

Independent function-by-function audit of `src/backend/executor/nodeCustom.c`
(custom-scan node executor) against the C, the c2rust rendering
(`c2rust-runs/backend-executor-nodeCustom/src/nodeCustom.rs`), and the port
(`crates/backend-executor-nodeCustom/src/lib.rs`). Re-derived from sources; the
port's comments and self-review were not trusted.

Verdict: **PASS**

## Function inventory (C source: 11 definitions)

| # | C function (line) | c2rust | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `ExecInitCustomScan` (26) | 3034 | `ExecInitCustomScan` (110) | MATCH | full control flow mirrored — see below |
| 2 | `ExecCustomScan` (static, 113) | 3079 | `ExecCustomScan` (230) + `exec_custom_scan_node` (90) | MATCH | CHECK_FOR_INTERRUPTS then provider dispatch |
| 3 | `ExecEndCustomScan` (124) | 3089 | `ExecEndCustomScan` (243) | MATCH/SEAMED | provider `EndCustomScan` |
| 4 | `ExecReScanCustomScan` (131) | 3093 | `ExecReScanCustomScan` (254) | MATCH/SEAMED | provider `ReScanCustomScan` |
| 5 | `ExecCustomMarkPos` (138) | 3097 | `ExecCustomMarkPos` (266) | MATCH | `!MarkPos` -> FEATURE_NOT_SUPPORTED error; else dispatch |
| 6 | `ExecCustomRestrPos` (149) | 3142 | `ExecCustomRestrPos` (284) | MATCH | `!RestrPos` -> error with same "...does not support MarkPos" text (deliberate C typo preserved) |
| 7 | `ExecCustomScanEstimate` (160) | 3187 | `ExecCustomScanEstimate` (306) | MATCH | `if EstimateDSM`: set pscan_len, estimate_chunk(BUFFERALIGN), estimate_keys(1) |
| 8 | `ExecCustomScanInitializeDSM` (173) | 3212 | `ExecCustomScanInitializeDSM` (328) | MATCH (folded) | see "Accepted divergence" |
| 9 | `ExecCustomScanReInitializeDSM` (189) | 3231 | `ExecCustomScanReInitializeDSM` (353) | MATCH (folded) | TOC lookup folded into provider seam |
| 10 | `ExecCustomScanInitializeWorker` (204) | 3249 | `ExecCustomScanInitializeWorker` (373) | MATCH (folded) | TOC lookup folded into provider seam |
| 11 | `ExecShutdownCustomScan` (220) | 3267 | `ExecShutdownCustomScan` (391) | MATCH/SEAMED | `if ShutdownCustomScan` guard preserved |

## Detailed re-derivation of MATCH samples

### ExecInitCustomScan
- `scanrelid = cscan->scan.scanrelid` — captured (123).
- `css = CreateCustomScanState(cscan)` — provider seam `create_custom_scan_state`
  alloc'd into `es_query_cxt` (132); the C lets the provider palloc.
- `css->flags = cscan->flags` (136); `ss.ps.plan = &cscan->scan.plan` aliasing the
  shared plan node (145); `ExecProcNode = ExecCustomScan` (146).
- `ExecAssignExprContext` (150).
- `if (scanrelid > 0)` open scan rel, set `ss_currentRelation` (157-160) — bound
  `> 0` matches C.
- `slotOps = css->slotOps; if (!slotOps) slotOps = &TTSOpsVirtual`
  -> `unwrap_or(Virtual)` (167).
- branch predicate `cscan->custom_scan_tlist != NIL || scan_rel == NULL`
  -> `!custom_scan_tlist_is_nil || !scan_rel_is_some` (178). NIL-list maps to
  empty/absent vec; correct.
  - then-branch: `ExecTypeFromTL` + `ExecInitScanTupleSlot`,
    `tlistvarno = INDEX_VAR(-3)` (182-186). `INDEX_VAR` const verified = -3
    (primnodes.h).
  - else-branch: `RelationGetDescr(scan_rel)` via `rd_att_clone_in`,
    `tlistvarno = scanrelid` (194-204).
- `ExecInitResultTupleSlotTL(&TTSOpsVirtual)` (209);
  `ExecAssignScanProjectionInfoWithVarno(tlistvarno)` (211).
- `qual = ExecInitQual(cscan->scan.plan.qual, css)` (217).
- `BeginCustomScan(css, estate, eflags)` (223). return css (225).

### ExecCustomScanEstimate (constants verified)
C `shm_toc_estimate_chunk` does `space_for_chunks += BUFFERALIGN(nbytes)` with
`ALIGNOF_BUFFER = 32`; c2rust shows the `& !(32-1)` mask. Port delegates to the
real owner `backend-storage-ipc-shm-toc`, whose `BUFFERALIGN` uses
`ALIGNOF_BUFFER = 32` and `wrapping_add(31) & !31` — exact match.
`estimate_keys(1)` matches `number_of_keys += 1`.

### ExecCustomMarkPos / ExecCustomRestrPos
Both: when the optional method pointer is absent, `ereport(ERROR,
errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("custom scan \"%s\" does not
support MarkPos", CustomName))`. Port builds `PgError::error(...)
.with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)` with the same message and reuses
the C's deliberate "MarkPos" wording in RestrPos too. Real ported logic, not a
deferral.

## Seam audit

Owned seam crate (by C-source coverage of nodeCustom.c):
`backend-executor-nodeCustom-seams`, declaring the 4 inward parallel-DSM entry
points (`exec_customscan_estimate/initialize_dsm/reinitialize_dsm/
initialize_worker`). All 4 are installed by this crate's `init_seams()`
(lib.rs 64-82) as mirror-PG-and-panic stubs (the opaque `PlanStateHandle`/
`ParallelContextHandle` cannot be resolved until the DSM owner can hand over the
owned `CustomScanState`; the owned entry points carry the real logic and are
callable directly). The recurrence guard's inverse check
(`every_declared_seam_is_installed_by_its_owner`) passes — no declared-but-unset
owned seam.

`init_seams()` is wired into `seams-init::init_all()` (line 67).

Outward seams consumed are all real cross-crate deps, thin marshal+delegate:
- provider callbacks (`create_custom_scan_state`, `begin/exec/end/rescan/markpos/
  restrpos/estimate_dsm/initialize_dsm/reinitialize_dsm/initialize_worker/
  shutdown`) -> `backend-nodes-extensible-seams`. These are extension-installed
  PROVIDER callbacks (the C `methods->X(...)`), correctly homed in extensible.c's
  seam crate and panicking until an extension installs them — there is no in-tree
  custom-scan provider. Mirrors the FDW-callback pattern.
- `exec_assign_expr_context`, `exec_open_scan_relation`,
  `exec_assign_scan_projection_info_with_varno` -> execUtils-seams.
- `exec_type_from_tl`, `exec_init_scan_tuple_slot`,
  `exec_init_result_tuple_slot_tl` -> execTuples-seams.
- `exec_init_qual` -> execExpr-seams.
- `check_for_interrupts` -> tcop postgres-seams (= CHECK_FOR_INTERRUPTS).
- `shm_toc_estimate_chunk` / `shm_toc_estimate_keys` -> shm-toc-seams (real owner).

No own-logic stub, no `todo!()`/`unimplemented!()`, no deferred/unsupported escape
on any node-owned path.

## Accepted divergence (not a FAIL)

`ExecCustomScanInitializeDSM` / `ReInitializeDSM` / `InitializeWorker`: the C
brackets the provider callback with `shm_toc_allocate`/`shm_toc_lookup` (+
`shm_toc_insert` for InitializeDSM) of an opaque storage-owned `void *coordinate`.
The port folds that allocate/lookup/insert into the provider seam (which receives
`pcxt`/`pwcxt` and reads the node's `plan_node_id`/`pscan_len`), because
`coordinate` is an opaque DSM chunk the node only brokers and the provider
callback is its only consumer. The `if (methods->X)` presence guard — the only
node-owned control flow — is preserved in-crate. The Estimate path, by contrast,
keeps its bracket logic (estimate_chunk + estimate_keys) in-crate. This is
behaviour-preserving and consistent with the repo's seam-the-opaque-broker
pattern (the whole parallel path is unreachable until a provider exists); noted,
not blocking.

## Gates

- `cargo check --workspace` — PASS.
- `cargo test -p backend-executor-nodeCustom` — PASS (0 tests; compiles).
- `cargo test -p seams-init` — PASS (2/2 recurrence-guard tests, incl. the
  declared-seam-installed inverse check).

## Verdict: PASS

Every function MATCH or correctly SEAMED per the rules; zero seam findings; init
wiring and recurrence guard green.
