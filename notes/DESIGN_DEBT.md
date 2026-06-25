# Design-debt ledger

Known, accepted debt that was reviewed and deliberately not fixed yet. Each
entry records location, description, and the intended fix. Remove entries as
they are paid down.

## `alloc_node` / `erase_lifetime` interning is UNSAFE-BY-PRECONDITION dressed as a safe `fn` — caller MUST `clone_in` the whole subtree into a durable arena first

- **Location:** `crates/types-pathnodes/src/lib.rs:3287` (`PlannerInfo::alloc_node`), which calls `Expr::erase_lifetime` (`crates/types-nodes/src/primnodes.rs:2549`, the sanctioned `unsafe transmute::<Expr<'mcx>, Expr<'static>>`). Same shape for the other ~6 erase helpers and all 113 `erase_lifetime` call sites.
- **Description:** `alloc_node(node: Expr<'mcx>) -> NodeId` is a *safe* signature but performs an unsafe lifetime erasure: it `erase_lifetime`s the node and pushes it into the index-addressed `node_arena` (`Vec<ArenaNode>`), declaring "the arena owns it for the PlannerInfo's life, so the `'mcx` brand is moot." The trap: **erasure only forgets the TOP node's brand — it does NOT deep-copy the node's `PgVec`/`PgBox` children.** So the safe signature silently asserts an un-checkable precondition: *the entire subtree already outlives the PlannerInfo*. If the caller hands a node whose children still live in a transient context (relcache-read `relcx`/`workcx`/`scratch`), the children are freed when that context drops, and the later `node_arena` drop double-frees → use-after-free SIGSEGV. **This is a real bug, not hypothetical:** it was the P0 regression from the expr-mcx merge (2026-06-21) — partition-key/stats/constraint Exprs interned via `alloc_node` while their backings were in the transient relcache context, crashing 6 partition-heavy regress files (insert/constraints/join/update/foreign_key/partition_join/partition_prune). Fixed by threading `run.mcx()` and `clone_in`-ing the surviving Exprs into the durable planner arena BEFORE `alloc_node` (commit `5e6be2a77`).
- **Fix (preferred):** make `alloc_node` take the durable `Mcx` and do the `clone_in` INTERNALLY (`alloc_node(mcx, node)` deep-copies into the arena's own context), so the precondition becomes impossible to violate — the function itself guarantees the durable copy, and the bug class becomes a compile error (caller can't pass a node that isn't `clone_in`'d). Alternatively mark it `unsafe fn` / rename `alloc_node_erasing` so every site visibly re-asserts the lifetime obligation. Either converts a silent UB footgun into a checked/visible one.
- **Related burn-down:** the 113 `erase_lifetime` call sites are the audit surface for this whole UAF class. A minority are legitimate process/cache-lifetime intern points; the majority are *transitional* bridge-crossings from the not-yet-fully-threaded `Expr<'static>` alias and should be converted to honest borrows or `clone_in` as threading completes. Each is a hand-checked "target outlives all uses" assertion the compiler does not verify. Drive the count down; treat any `erase_lifetime` near a transient/relcache context as suspect.

## CHECK FOR RE-ENTRANCY: per-backend state behind a non-reentrant `RefCell`/`Mutex` can double-borrow/deadlock on a signal/interrupt re-entry

- **Class (cross-cutting review item, not a single site).** Where the C source
  keeps per-backend mutable state in plain globals (often `volatile
  sig_atomic_t`) that a signal handler / `CHECK_FOR_INTERRUPTS` path may touch
  *while the main code is mid-update*, the Rust port frequently wraps that state
  in one `thread_local! RefCell<T>` (or a `Mutex<T>`). C tolerates the re-entry
  because it is only reading/writing an int with no "borrow" held; the Rust cell
  does **not** — if the interrupt/handler re-enters and borrows the cell while
  the outer borrow is still live, `RefCell` panics (`already borrowed`) and
  `Mutex` deadlocks/poisons. There is **no sound re-entrant `&mut` cell** (it
  would alias `&mut` = UB); `std::sync::ReentrantLock` / `parking_lot::
  ReentrantMutex` are re-entrant for **shared `&T` only**, so they do not solve
  the mutate-on-re-entry case. Same family as the port-introduced
  unwind/cleanup-path escalations (non-reentrant Mutex/RefCell in
  abort/unwind/hook paths).
- **Concrete instance (RESOLVED):** `transam_parallel/src/lib.rs`
  (`with_globals`). Parallel teardown
  (`wait_for_parallel_workers_to_finish` -> `ExecParallelFinish`) held a
  `with_globals` borrow when the interrupt path re-entered `with_globals` ->
  `RefCell already borrowed` panic. **Fixed:** `ParallelMessagePending` was
  pulled out of `RefCell<ParallelGlobals>` into a standalone
  `static PARALLEL_MESSAGE_PENDING: Cell<bool>` (lib.rs:320-332), mirroring C's
  `volatile sig_atomic_t`; the signal handler now `.set()`s a `Cell` and never
  takes a `borrow_mut()`.

### TD-REFCELL-REENTRANCY — tree-wide audit (2026-06-24)

A systematic audit of every backend-global `RefCell` borrow site in the
re-entrancy-prone subsystems (catalog caches, plan cache, GUC store,
parallel-globals, portal/snapshot registries, fmgr/expr caches, pgstat,
RI-trigger & locale caches, sinval, procsignal) was run. **Conclusion: the four
known production repros are all already fixed, and the structural
snapshot-under-momentary-borrow-then-drop pattern is applied consistently across
the caches.** Detail:

- **Fixed in this audit (the one genuine HIGH found):**
  `applyparallelworker/src/lib.rs` had `parallel_apply_message_pending` (C's
  `volatile sig_atomic_t ParallelApplyMessagePending`) living inside the
  `RefCell<Globals>` while `HandleParallelApplyMessageInterrupt()` (a signal
  handler) set it through `with_globals(|g| g.… = v)` → `borrow_mut()`. Identical
  bug class to the transam_parallel `ParallelMessagePending` one. Fixed by moving
  it to a standalone `static PARALLEL_APPLY_MESSAGE_PENDING: Cell<bool>`; the
  accessors now `.get()`/`.set()`, async-signal-safe.
- **Confirmed already-protected (no change needed):** inval registry
  (`CallSyscacheCallbacks`/`CallRelSyncCallbacks`/`AcceptInvalidationMessages` all
  snapshot the callback list out under a momentary `with_state` then iterate
  unborrowed), plancache (`PlanCacheRelCallback`/`ObjectCallback`/`ResetPlanCache`
  clone `saved_plan_list` then operate on per-entry `Rc<RefCell>`), catcache
  (`with_arena` borrows are momentary; every catalog scan runs unborrowed; docs
  cite the hazard), syscache/typcache/relcache `with_state` (every `lookup_pg_type`
  / `SearchSysCache` / `relation_open` is OUTSIDE the closure), the GUC store
  (`with_store_mut` defers every re-entrant assign hook past the borrow via
  `DeferredAssignHook`; `prohibitValueChange` reads `c.value` not a store
  accessor), funccache/evtcache/spccache/attoptcache/ts_cache/relfilenumbermap,
  portalmem (`with_portal_globals`/`with_active_portal` replace-out-then-run),
  fmgr_core/fmgr_dfmgr (`_PG_init` runs unborrowed), RI-trigger + locale caches,
  snapmgr, sinval, procsignal (all `Cell`).
- **Remaining MED seatbelt candidates (NOT exploitable today — safe only by the
  invariant that all current callers' closures are non-re-entrant; tracked for a
  future `try_borrow_mut → Err(PgError)` seatbelt or structural narrowing):**
  1. `crates/_support/types/fmgr/src/mat_srf.rs:161` `with_top` — holds
     `borrow_mut()` across an arbitrary caller closure `f(stack.last_mut())`. Safe
     only because every in-tree SRF sink closure (execSRF/functions/plpgsql/contrib)
     is emit-only; a future value-per-call SRF that recursed into another SRF from
     inside the sink closure would re-enter `MAT_SRF_STACK` and panic. 12 call
     sites return generic `R` (not `PgResult`), so a `try_borrow` conversion is
     invasive; left as a documented invariant.
  2. pgstat `with_local`/`with_pending`/`with_snapshot`
     (`activity_pgstat/src/local.rs:97/102/108`) and the per-subsystem `with_*`
     helpers — same `borrow_mut`-across-`f` shape; no re-entrant caller today
     (counter-mutation closures), but the generic helper signature invites one.
  3. relcache per-entry `RelationRef::with_mut`/`with_relation_mut`
     (`relcache/src/core_entry_store.rs`) — hand out a `&mut` borrow scoped to a
     caller closure; safe today because no caller re-enters the SAME entry (e.g.
     a relcache rebuild or catalog lookup of the same relid) inside that closure.
- **Review checklist when porting/auditing a crate that holds per-backend state
  in a `RefCell`/`Mutex` thread_local:** (1) Can a signal handler,
  `CHECK_FOR_INTERRUPTS`, an ereport/elog callback, a `Drop`/abort path, or a
  cache-invalidation hook re-enter this cell while an outer borrow is live? (2)
  Does the C original use a `volatile`/`sig_atomic_t` global here (a tell that
  the field is *meant* to be touched re-entrantly)? If either holds, it is a
  latent double-borrow/deadlock.
- **Fix (faithful to C — do NOT hunt for a re-entrant cell):** the
  signal/interrupt-reachable field is conceptually one of C's `volatile`
  globals, so give it its own standalone `Cell<T>` (Copy fields) or atomic
  (`AtomicBool`/`AtomicU32`), separate from the monolithic `RefCell` — re-entry
  then uses `get`/`set`/`load`/`store`, never holding a live borrow. Or narrow
  the borrow so no `RefMut` spans the wait/`CHECK_FOR_INTERRUPTS`/handler point
  (take/replace the value into a local, operate, put it back). Prefer
  `thread_local!`+`Cell`/atomic over a coarse `RefCell` for any per-backend
  state on a signal/interrupt-reachable path.

## TD-REWRITEHANDLER-RULELOCK: STEP 1 (rd_rules carrier on the relcache ENTRY) DONE; STEP 2 (port rewriteHandler.c) BLOCKED on a 2nd carrier-layer keystone (surface rd_rules onto the trimmed per-query `types_rel::Relation<'mcx>` handle / a relcache `relation_rules` reader seam) — CONTRACT_RECONCILE_PENDING

- **Update (full-Query cache-ownership keystone landed):** STEP 1 — the
  prerequisite that blocked this entry — is now PAID. The `rd_rules`
  RuleLock/RewriteRule carrier holding whole `Query` trees is real and proven
  sound, via a leaked process-lifetime **CacheMemoryContext** `Mcx<'static>`
  arena (the C `CacheMemoryContext` rendering). What remains is STEP 2 (porting
  rewriteHandler.c itself + re-keying the `query_rewrite` seam).
- **What landed (STEP 1):**
  - `crates/types-relcache-entry/src/lib.rs`: new value-typed `RuleLock { rules:
    PgVec<'static, RewriteRule> }` and `RewriteRule { ruleId, event: CmdType,
    enabled: u8, isInstead: bool, qual: Option<PgBox<'static, Node<'static>>>,
    actions: PgVec<'static, Query<'static>> }`. `RelationData.rd_has_rules: bool`
    REPLACED by `rd_rules: Option<PgBox<'static, RuleLock>>` (the C `RuleLock
    *rd_rules`; `None` is the C NULL). The crate gained a direct `types-nodes`
    dep (acyclic — types-nodes does not depend on relcache-entry).
  - `crates/backend-utils-cache-relcache/src/derived.rs`: `RelationBuildRuleLock`
    is now REAL — scans `pg_rewrite` (genam `relcache_scan_pg_rewrite` seam),
    `stringToNode`s each `ev_qual`/`ev_action` (read.c `string_to_node` seam,
    INSTALLED by backend-nodes-core) into the cache arena, builds the rule list,
    sorts by `ruleId`, stores the `RuleLock`. New `cache_memory_context() ->
    Mcx<'static>` leaked-context accessor (mirrors backend-utils-init-postinit's
    `TopMemoryContext` leak). The old `nodexform_seam::rule_lock` acknowledgement
    seam was RETIRED (decl removed; no installer/caller remained).
  - `build.rs`/`invalidate.rs`/`initfile.rs`: `rd_has_rules` references migrated
    to `rd_rules` (presence checks, the rebuild `SWAPFIELD(rd_rules)` preserve,
    the init-file fresh-reset). Soundness proven by two unit tests in derived.rs
    (`cache_ownership_keystone_tests`): a `Query<'static>` built in the arena
    survives its building scope, and a lifetime-free `RelationData` owns a
    `RuleLock` of whole `Query` action trees.
  - `crates/backend-access-index-genam-seams`: new `ScannedPgRewrite` DTO +
    `relcache_scan_pg_rewrite` seam (the genam catalog-scan primitive). Added to
    the seams-init declared-but-uninstalled allowlist beside its sibling
    `relcache_scan_pg_index`/`pg_constraint`/`pg_statistic_ext` scans — the
    genam owner has only the DTO, not the scan body, so it loud-panics
    (mirror-PG-and-panic) until genam ports the pg_rewrite scan-and-decode.
- **Design decision (the keystone answer):** the C `CacheMemoryContext` is a
  process-lifetime context that never gets freed; the faithful Rust rendering is
  a **leaked** (`Box::leak`) `MemoryContext` whose `Mcx<'static>` handle clones
  freely. `Query<'static>` trees live there, and a lifetime-free cache entry may
  own them because they borrow nothing from any per-query `'mcx`. This is the
  same already-proven pattern as the leaked `TopMemoryContext` and the `'static`
  `rd_amcache` slot — NOT an invented handle/registry. The older note's claimed
  blockers are stale: (a) `copy_query::Query<'mcx>` is now the full 49-field
  model with a typed `clone_in` (the K1 node-model keystone landed), and (b)
  `MemoryContext` is a real owned value (not a `CtxId` u64), so a `'static`
  arena is achievable.
- **STEP 2 still pending (the actual rewriteHandler.c port):** rewriteHandler.c
  (4655 LOC, ~35 fns: QueryRewrite/RewriteQuery/fireRIRrules/fireRules/
  ApplyRetrieveRule/rewriteRuleAction/rewriteTargetListIU/rewriteTargetView/
  matchLocks/get_view_query/relation_is_updatable/…) can now read
  `rd_rules.rules[i].{event,qual,actions,isInstead,enabled}` off an open
  relation. What remains: port the rule-application engine into
  `backend-rewrite-core` over the now-available carrier, install the 6
  rewritehandler seams real, and re-key the `query_rewrite` seam (today
  `query_rewrite<'mcx>(mcx, query: portalcmds::Query) -> PgResult<PgVec<
  portalcmds::Query>>` over the **opaque** `portalcmds::Query`) + its sole
  consumer (`backend-commands-portalcmds` `PerformCursorOpen`) onto the
  canonical `copy_query::Query`. That contract reconciliation + the engine port
  is the remaining work; the carrier no longer blocks it.
- **Reading the rule tree across a seam (STEP 2 sub-note):** the carrier lives
  on the OWNED relcache entry (`backend-utils-cache-relcache`), reachable in-crate
  via `with_rel`. rewriteHandler (a different crate) will need a relcache-seams
  reader exposing `rd_rules` (the rules' `event`/`qual`/`actions` as borrowed
  `&Query<'static>`/`&Node<'static>` values, or re-projected into the caller's
  `mcx`), mirroring how the trigdesc/partition payloads are surfaced. That reader
  is STEP-2 scope.

- **RE-VERIFIED 2026-06-16 (STEP 2 attempt) — STEP 2 is itself keystone-blocked
  by a SECOND carrier layer; reclassified CONTRACT_RECONCILE_PENDING / not
  buildable-now.** STEP 1 surfaced `rd_rules` on the LONG-LIVED relcache entry
  type (`types_relcache_entry::RelationData` /
  `core_entry_store::entry::RelationData`, the `with_rel` target). But the
  rewriteHandler engine and ALL SIX rewritehandler-seams operate on the
  **trimmed per-query handle** `types_rel::Relation<'mcx>` returned by
  `table_open` (`relation_open` -> `RelationIdGetRelation` -> `project_entry`).
  That handle is a DIFFERENT struct: `types_rel::RelationData<'mcx>` has NO
  `rd_rules` field, `types-rel` deliberately has NO `types-nodes` dep (the
  trimmed cross-unit slice is node-vocabulary-free by design — same invariant the
  original STOP cited), and `build.rs::project_entry` (the owned-entry ->
  trimmed-slice projection) does NOT copy the rule trees. So every engine read of
  `relation->rd_rules` / `view->rd_rules` (matchLocks / fireRIRrules /
  ApplyRetrieveRule / get_view_query) is UNREACHABLE through the handle the
  rewriter holds. The `get_view_query` seam's own signature proves it:
  `view: &types_rel::Relation<'mcx>` (the trimmed slice, no rules).
- **The actual STEP-2 prerequisite keystone (relcache/types-rel-owned, NOT
  rewriteHandler-owned):** a relcache reader that surfaces the rules to a
  `Relation<'mcx>`/`Oid` consumer. Two shapes, both relcache-layer work:
  (A) add `rd_rules` to `types_rel::RelationData<'mcx>` (forces `types-rel` ->
  `types-nodes` dep + re-projecting/cloning the cached `Query<'static>` trees
  into `'mcx` in `project_entry`) — re-architects the trimmed slice; or
  (B) a new `relation_rules(mcx, reloid) -> PgResult<Option<RuleLockImage<'mcx>>>`
  reader seam declared by rewriteHandler + a new mcx-bound `RuleLockImage<'mcx>`/
  `RewriteRuleImage<'mcx>` DTO (rules re-projected via `Query::clone_in(mcx)`),
  **installed in the relcache crate** (`with_relation` + clone_in is sufficient;
  relcache already deps `types-nodes`). (B) is the lighter, model-preserving path
  and matches this note's own "reader is STEP-2 scope" — but it is a relcache
  installer addition (different owner) plus re-keying every engine rd_rules read
  to re-fetch by Oid, which is the keystone, not the rewriteHandler port itself.
- **Buildable-now subset is the SAME insufficient leaf set the original STOP
  named:** `build_column_default`, the tlist-IU/VALUES-DEFAULT family
  (`rewriteTargetListIU`/`process_matched_tle`/`get_assignment_input`/
  `searchForDefault`/`findDefaultOnlyColumns`/`rewriteValuesRTE(ToNulls)`), and
  the view-updatability ANALYSIS family (`view_query_is_auto_updatable` et al,
  which take a passed-in `viewquery`). Their callees are all present (verified:
  `get_typdefault` seam, `coerce_to_target_type`/`coerce_null_to_domain`
  backend-parser-coerce, `getIdentitySequence` pg-depend, `TupleDescGetDefault`
  tupdesc, `exprType` backend-nodes-core; no cycles). But landing only these does
  NOT deliver the unit's deliverable (the `QueryRewrite` entry + the 6 seam
  installs + the `query_rewrite` contract collapse) — all 6 seams would still
  seam-and-panic. Per "don't land insufficient leaves / no hollow shell", left
  for the keystone fill.
- **query_rewrite contract still pending (unchanged):** seam is still
  `query_rewrite<'mcx>(mcx, query: portalcmds::Query) -> PgResult<PgVec<
  portalcmds::Query>>` over the opaque `portalcmds::Query`; consumer
  `backend-commands-portalcmds` `PerformCursorOpen` (line ~144) wired to it. The
  re-key onto canonical `copy_query::Query` lands WITH the engine (pointless to
  re-key while the engine that fills it can't be implemented).

## Datum physical-representation keystone (F0) DONE — producer waves pending

- **Location:** `crates/types-tuple/src/backend_access_common_heaptuple.rs`
  (`enum Datum<'mcx>`); trait re-homed to
  `crates/types-datum/src/expandeddatum.rs` (`ExpandedObject`,
  `flatten_expanded`).
- **Status (F0 landed):** The canonical value enum `types_tuple::Datum<'mcx>`
  was widened ADDITIVELY from `ByVal(usize) | ByRef(PgVec<u8>)` to the full
  physical-representation set: `+ Cstring(String)`, `+ Composite(FormedTuple)`,
  `+ Expanded(Box<dyn ExpandedObject>)`, `+ Internal(Box<dyn Any>)` (mirrors the
  fmgr-boundary `RefPayload` arms). The `ExpandedObject` trait was relocated
  from `types-fmgr` down to `types-datum` (both `types-tuple` and `types-fmgr`
  depend on it; `types-fmgr` re-exports for back-compat) to avoid a layering
  cycle. `Clone`/`Debug`/`PartialEq`/`Eq` are hand-implemented (the trait-object
  arms are not derivable): `Expanded` flattens-on-compare like `RefPayload`,
  `Internal` is non-comparable / non-bare-Clone (panics, no producer reaches it).
- **Producer waves PENDING (this F0 only made the arms expressible):** Nothing
  yet CONSTRUCTS the 4 new arms, so every exhaustive match across the workspace
  routes them to the site's existing rejection/error/`false` arm or a
  `"... not yet produced — wave 2"` panic (sanctioned mirror-and-panic, the arm
  is genuinely unreachable). The composite-Datum bridges that listed this enum's
  missing `Composite` arm as a blocker (`record_from_values`,
  `get_expr_result_type_node`, `DatumGetHeapTupleHeader`/`HeapTupleGetDatum`,
  the misc2 `make_expanded_object_read_only_internal` byte-image divergence
  below) are now type-level UNBLOCKED but still need their producer/consumer
  seams ported. Pay down by landing the producers (heap_form_tuple→Composite,
  fmgr cstring/expanded I/O, internal pseudo-type sites).

## json unique-key check uses the global allocator, not a charged context

- **Location:** `crates/backend-utils-adt-json/src/lib.rs`
  (`JsonUniqueCheckState`, `JsonUniqueBuilderState`, `JsonUniqueParsingState`,
  `json_unique_check_key`, and the `key_bytes`/throwaway `.to_vec()` copies in
  the object-agg / json_build_object workers).
- **Description:** C's uniqueness check is a `dynahash` table (and a throwaway
  `StringInfo`) allocated in the aggregate / parse memory context. The port
  keeps the entries and the throwaway key bytes in std `alloc::Vec<u8>`
  (global allocator, infallible) rather than context-charged `PgVec`, because
  the parser semantic-action callbacks (`json_unique_object_*`) are driven
  through the `common-jsonapi` seam with no `Mcx` in scope, and the
  `JsonUniqueParsingState` is `Default`-constructed there. The exact match
  function (object_id, key_len, bytes) carries correctness; the table is
  transient (one object/build/parse).
- **Fix:** When `common-jsonapi` lands and the parse path can thread an `Mcx`
  into the semantic-action state, make `JsonUniqueCheckState` hold a
  context-charged `PgVec`-backed table (or route through the ported
  `backend-utils-hash-dynahash`) so the key copies become fallible
  context-charged allocations matching the C palloc domain.


## GetLWTrancheName allocates an owned String per call

- **Location:** `crates/backend-storage-lmgr-lwlock/src/lib.rs`
  (`GetLWTrancheName`, `GetLWLockIdentifier`, the `t_name` error-path
  helper).
- **Description:** C's `GetLWTrancheName` returns a stored `const char *`
  without allocating; the port clones an owned `String` on every call
  (including every wait-event identifier lookup and every "lock %s is not
  held" error path) through the infallible global allocator.
- **Fix:** Return `&'static str` for builtin tranches and a cheap shared
  handle (e.g. `Arc<str>` stored in the thread-local registry) for dynamic
  ones; reserve owned copies for the registry insert path.

## AmopRow/AmprocRow duplicated across types layers

- **Location:** `crates/types-hash/src/backend_access_hash_hashvalidate.rs:45-58`
  vs `crates/types-amvalidate/src/backend_access_index_amvalidate.rs:14-28`
  (used together by `backend-utils-cache-syscache-seams` and
  `backend-access-index-amvalidate-seams`).
- **Description:** Two distinct trimmed mirrors of the same
  pg_amop/pg_amproc rows exist at two layers; the hashvalidate consumer
  converts between them by hand, so a field added to one can silently
  desync from the other.
- **Fix:** Keep one shared trimmed row pair in `types-amvalidate` (a
  superset of both) and have the syscache seams and
  `identify_opfamily_groups` use it.

## Zero-arg getter seams for owner-held per-backend GUCs/flags

- **Location:** `crates/backend-utils-init-small-seams/src/lib.rs`
  (`work_mem`; `my_proc_number`, read at the consumer boundary by
  `backend-storage-ipc-dsm-core` and `backend-utils-activity-small` to feed
  the lwlock acquire surfaces' explicit `my_proc_number` parameter),
  `crates/backend-utils-activity-status-seams/src/lib.rs`
  (`track_activities`), `crates/backend-utils-activity-pgstat-seams/src/lib.rs`
  (`shmem_is_shutdown`).
- **Description:** Zero-arg getters for another unit's globals — the named
  smell class. Mitigating: each targets the global's actual owner, mirrors a
  point-of-use C read of a per-backend GUC/flag, and the owner will hold the
  state `thread_local`, so these are the legitimate owner's-seam shape.
- **Fix:** Keep for now; prefer passing the value as a parameter where the
  call site's caller already has it. Revisit when a GUC-access design lands.

## LOCKMODE stays a C-faithful `i32` alias

- **Location:** `crates/types-storage/src/lock.rs`.
- **Description:** C is `typedef int LOCKMODE` plus `#define`s, so the alias
  is faithful (opacity inherited, not invented). The earlier duplicate
  (`types-tuple::access`, with only two modes) was removed and all seams now
  point at the full `types-storage` table.
- **Fix:** Optionally upgrade to a closed `LockMode` enum (NoLock..
  AccessExclusiveLock) if a consumer ever needs exhaustive matching; note
  `LOCKMASK` bit math (`1 << mode`) must keep working.

## combocid: local mirrors of shmem.c's add_size / mul_size

- **Location:** `crates/backend-utils-time-combocid/src/lib.rs`
  (`add_size` / `mul_size` / `size_overflow`).
- **Description:** `add_size`/`mul_size` are `storage/ipc/shmem.c`-owned
  overflow-checked size helpers re-implemented as local private mirrors
  (`EstimateComboCIDStateSpace` needs them). AGENTS.md's rule is that the only
  acceptable missing piece for an unported owner is a loud-panic seam; a local
  mirror is a second implementation that can drift from the owner's
  (same SQLSTATE `ERRCODE_PROGRAM_LIMIT_EXCEEDED`, same message text today).
- **Fix:** when the `backend-storage-ipc-shmem` owner lands, delete the
  mirrors and take a direct cargo dependency on the owner's `add_size` /
  `mul_size` (no cycle expected: combocid -> shmem is the C dependency
  direction); alternatively move them into a shared `types-*`/util layer the
  owner installs from, in the same change that ports shmem.c.

## set_latch_my_latch encodes MyLatch into the latch unit's seam

- Location: `crates/backend-storage-ipc-procsignal/src/lib.rs`
  (`procsignal_sigusr1_handler`, consuming
  `crates/backend-storage-ipc-latch-seams/src/lib.rs` `set_latch_my_latch`)
- Description: `set_latch_my_latch()` bakes `SetLatch(MyLatch)` — a
  `globals.c` per-backend pointer — into the latch unit's seam as a zero-arg
  ambient operation. Defensible for the SIGUSR1 handler, which genuinely has
  no parameter source, but the signature commits the latch owner to ambient
  state.
- Fix: when the latch unit lands, prefer `set_latch(latch: &Latch)` with an
  owner-side my-latch accessor for handler contexts, or document the
  handler-only exception on the seam. Revisit before more consumers copy the
  shape.

## TD-LATCH-PROC-BRIDGE: SetLatch-by-proc seams need the latch<->proc handle bridge

- Location: `crates/backend-storage-ipc-latch-seams/src/lib.rs`
  (`set_latch_for_procno`, `set_latch_by_proc_number`, `set_latch_for_proc_pid`)
  vs owner `crates/backend-storage-ipc-latch/src/lib.rs` (`SetLatch` /
  `lookup_latch` / `LATCHES` registry) and `crates/backend-storage-lmgr-proc`
  (`proc_latch` -> `proc_latch_handle`).
- Description: these three seams set ANOTHER backend's PGPROC-embedded
  `procLatch`, named by proc number (or PID). The latch owner identifies a
  latch only by its slot in this crate's own append-only `LATCHES` registry
  (`lookup_latch` `.expect("invalid LatchHandle")` on anything not minted by
  `allocate_latch`). proc.c's `proc_latch(procno)` seam returns a `LatchHandle`
  minted from the proc number (`proc_latch_handle(procno)`) — a DIFFERENT,
  unregistered handle space — so `SetLatch(proc_latch::call(procno))` would
  resolve a handle the latch registry never allocated and panic. This is the
  unbuilt latch<->proc PGPROC-latch integration bridge: proc.c's own
  `set_proc_latch` already `panic!`s on exactly this boundary
  ("latch <-> proc PGPROC-latch bridge not yet wired"). `set_latch_for_proc_pid`
  is additionally blocked on procarray.c (unported, CATALOG status `todo`) for
  the PID->proc-number lookup (`BackendPidGetProc` + `GetNumberFromPGProc`,
  surfaced as `backend_pid_get_proc_role`). Tracked in
  `CONTRACT_RECONCILE_PENDING` (3 entries) rather than force-wired with a handle
  from the wrong space.
- Fix: build the bridge — register each PGPROC's `procLatch` into the latch
  registry at proc setup (or unify the two handle spaces) so `proc_latch`
  returns a registry handle — then install all three seams in the latch unit's
  `init_seams` (the bodies are a one-line `SetLatch(proc_latch::call(procno))`,
  with the PID variant first resolving via `backend_pid_get_proc_role`). Land
  procarray for the PID path.

## ProcSignalShmemInit allocates the slot array infallibly

- Location: `crates/backend-storage-ipc-procsignal/src/lib.rs`
  (`ProcSignalShmemInit`)
- Description: C's failure surface is `ShmemInitStruct`'s out-of-shared-memory
  `ereport(ERROR)`; the port allocates the slot array with infallible
  `Vec::with_capacity`/`into_boxed_slice` (process abort on OOM) inside
  `OnceLock::get_or_init`. Not mcx territory (shared memory, not a memory
  context), so it waits on the shmem allocator unit.
- Fix: when the shmem allocator unit lands, route the slot-array allocation
  through its fallible API and surface its `ereport(ERROR)` via the existing
  `PgResult`.

## Wait-event constants are transcribed per-crate

- Location: `crates/backend-storage-ipc-procsignal/src/lib.rs`
  (`WAIT_EVENT_PROC_SIGNAL_BARRIER`)
- Description: `WAIT_EVENT_PROC_SIGNAL_BARRIER` is hand-derived as
  `PG_WAIT_IPC | 0x2A` from the alphabetized position in
  `wait_event_names.txt` (verified against 18.3: 0-based index 42 in the
  `WaitEventIPC` section; the generator assigns the first name the class
  value `PG_WAIT_IPC` itself). Positional transcription is a silent-divergence
  risk as more crates copy wait events.
- Fix: move wait-event class masks and the per-class event indices into a
  shared `types-*` wait-event module generated/checked once against
  `wait_event_names.txt`; the doc-comment off-by-one wording is already fixed
  in-crate.

## Allocation discipline — infallible state-context creation in `with_state`

- **Location:** `crates/backend-utils-cache-syscache/src/lib.rs` (`with_state`,
  the `.expect("allocating the empty syscache state cannot fail")`);
  `crates/backend-utils-cache-ts-cache/src/lib.rs` (`with_state`, same pattern).
- **Description:** `with_state` lazily creates the per-backend state context +
  struct and unwraps the fallible `McxOwned::try_new` with `.expect`. The
  context plus state allocation is a real allocation; the C counterpart
  (`CreateCacheMemoryContext` + the file statics' first touch) sits on an
  `ereport(ERROR)`-capable path, so the faithful surface is `Err(PgError)`
  with `ERRCODE_OUT_OF_MEMORY`, not an abort.
- **Fix:** make `with_state` fallible (propagate via `mcx.oom`) once the
  infallible call sites (`bool` predicates, invalidation callbacks) have a
  sanctioned error path, or initialize the state at backend-init time where a
  `PgResult` can propagate.

## Allocation discipline — owned std `Vec`/`String` workspaces

- **Location:** `crates/backend-utils-cache-ts-cache/src/lib.rs`
  (`getattr_name` -> `String`; `maplists`/`mapdicts` `Vec::with_capacity`
  workspaces in `lookup_ts_config_cache`; `parts: Vec<&str>` in
  `getTSCurrentConfig`/`check_default_text_search_config`; the
  `buf.as_str().to_owned()` GUC store);
  `crates/backend-utils-cache-syscache/src/lib.rs` (`SysCacheGetAttrNotNull`
  error-message `String` building).
- **Description:** transient workspaces and name extraction use infallible std
  allocation (abort-on-OOM) where C uses stack arrays (`maplists`/`mapdicts`)
  or palloc (ereport-on-OOM). Mostly error-message paths, so behavioral
  exposure is small.
- **Fix:** move the workspaces to mcx-backed `PgVec`/`PgString` with
  `try_reserve`; error-message-path `String`s may stay with a comment.

## Syscache projection seams where a direct dep now works

- **Location:** `crates/backend-utils-cache-syscache-seams/src/lib.rs`
  (`search_relation_relam` / `search_opclass` / `search_amop_list` /
  `search_amproc_list`); consumers `crates/backend-access-hashvalidate`,
  `crates/backend-executor-execAmi`.
- **Description:** `backend-utils-cache-syscache` is merged, and neither
  consumer forms a cargo cycle with it — per the seam rules ("a seam exists
  only where a direct dep would create a cycle") these caller-shaped
  projected-row seams are no longer justified, and the one-micro-seam-per-
  caller-field shape proliferates.
- **Fix:** convert hashvalidate/execAmi to direct deps on
  `backend-utils-cache-syscache` and retire the projection seams (keep one
  only if a genuinely cyclic caller appears).

## DD-1: ExprContext callback removal keys on Rust fn-pointer equality

- **Location**: `crates/backend-executor-execUtils/src/lib.rs`
  (`UnregisterExprContextCallback`; `#![allow(unpredictable_function_pointer_comparisons)]`).
- **Description**: `UnregisterExprContextCallback` removes entries whose
  `(function, arg)` pair matches by Rust `fn`-pointer `==`, mirroring the C
  address comparison. Unlike C function addresses, Rust fn items can be
  merged (identical bodies) or duplicated (per-codegen-unit instantiation),
  so the comparison can in principle remove the wrong callback or miss the
  right one.
- **Fix**: key registrations by an explicit token — have
  `RegisterExprContextCallback` return a small `CallbackId` that
  `UnregisterExprContextCallback` takes — and migrate the (currently C-shaped)
  callers when the first real unregistering consumer lands.

## DD-2: `get_rte_permission_info` seam returns a positional index

- **Location**: `crates/backend-parser-relation-seams/src/lib.rs`
  (`get_rte_permission_info`), consumed by
  `crates/backend-executor-execUtils/src/lib.rs` (`GetResultRTEPermissionInfo`).
- **Description**: the seam returns a bare `usize` index into
  `EStateData::es_rteperminfos` where C's `getRTEPermissionInfo` returns the
  `RTEPermissionInfo *` node. The raw index is only valid against one
  specific `PgVec` and nothing ties them together.
- **Fix**: restructure the seam around the lookup key the C function actually
  uses (`rte->perminfoindex`, a 1-based `Index` already carried on
  `RangeTblEntry`), or return a `PermInfoId` newtype scoped to the EState,
  when the parse_relation owner lands and fixes the signature it will install.

## dsm_impl: `impl_private` enum drops the C allocation's OOM path

- **Location**: `crates/backend-storage-ipc-dsm-core/src/dsm_impl.rs`
  (`DsmImplPrivate`, module header).
- **Description**: C's System V implementation heap-allocates an `int` in
  `TopMemoryContext` for `void *impl_private` and can therefore raise
  `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)` there; the Rust port stores the shm
  ident inline as an enum variant (rule-6-conformant — the `void *` resolves
  to its real payload), which removes that allocation and narrows the failure
  surface. Documented in the module header; behavior otherwise identical and
  the overall surface stays `PgResult` via `dsm_impl_op`.
- **Fix**: none planned beyond keeping the note; revisit only if a consumer
  ever depends on that specific OOM edge.

## Infallible `format!` on transam WARNING paths

- **Location:** `crates/backend-access-transam-transam/src/lib.rs` —
  `elog(WARNING, format!("no pg_subtrans entry for subcommitted XID ..."))`
  in `TransactionIdDidCommit` and `TransactionIdDidAbort`.
- **Description:** `format!` is infallible heap allocation on a path whose C
  counterpart (`errmsg` palloc in ErrorContext) treats OOM as an ereport, not
  an abort. This follows backend-utils-error's existing
  `elog(level, impl Into<String>)` message channel, which is not yet
  mcx-backed/fallible.
- **Suggested fix:** No local action; migrate these call sites when
  backend-utils-error's message channel goes mcx-backed/fallible in the
  repo-wide migration pass.
- **Branch:** port/backend-access-transam-transam

## OutputFileName round-trips through UTF-8

- **Location**: `crates/backend-utils-init-small/src/globals.rs`
  (`SetOutputFileName`) and the backing store
  `backend_utils_error::config::output_file_name` (`Option<String>`).
- **Description**: C's `OutputFileName` is a `char[MAXPGPATH]` byte buffer
  and unix paths are not guaranteed UTF-8.
  `String::from_utf8_lossy(...).into_owned()` in `SetOutputFileName` silently
  corrupts non-UTF-8 path bytes; the representation divergence is otherwise
  unledgered.
- **Suggested fix**: store the value as bytes (`Vec<u8>` or the fixed
  `[u8; MAXPGPATH]` buffer) in `backend_utils_error::config` and convert at
  the `open()` boundary (`OsStrExt::from_bytes`), or explicitly ledger the
  UTF-8 assumption for all path-valued globals. Deferred here because the
  store and its reader (`DebugFileOpen` in the audited `backend-utils-error`
  crate) change together.
- **Branch**: port/backend-utils-init-small

## `set_my_client_socket` returns `()` but stands for an ereport-capable body

- **Location:** `crates/backend-utils-init-small-seams/src/lib.rs`
  (`set_my_client_socket`)
- **Description:** The C body this seam stands for (`MyClientSocket =
  palloc(sizeof(ClientSocket)); memcpy(...)` in `launch_backend.c`) allocates
  via `palloc`, which can `ereport(ERROR)` on out-of-memory. The seam returns
  bare `()`, so that error path has no Rust representation.
- **Suggested fix:** When `PgResult`/`Mcx` land in this repo, change the seam
  to `set_my_client_socket(...) -> PgResult<()>` (and thread the allocator if
  the mctx design requires it), propagating at the call site in
  `backend-postmaster-launch-backend`.
- **Why deferred:** `PgResult` and the memory-context machinery do not exist
  on this branch yet; there is no error currency to return.
- **Branch:** `port/backend-postmaster-launch-backend`

## backend-catalog-namespace: per-backend `NamespaceState` remains std-allocated

- **Location**: `crates/backend-catalog-namespace/src/lib.rs` (`NamespaceState`,
  the `STATE` thread_local, and its accessors `active_search_path()` /
  `namespace_search_path()`).
- **Description**: The branch threads `Mcx<'mcx>` through every function whose
  C counterpart allocates in `CurrentMemoryContext` (results, catalog-row seam
  copies, transient parse lists), but the C file-scope statics —
  `baseSearchPath`/`activeSearchPath` (TopMemoryContext lists), the
  `SearchPathCache` (`SearchPathCacheContext`), and the `namespace_search_path`
  GUC string — live in a `thread_local!` `NamespaceState` whose containers are
  std `Vec`/`String`/`HashMap`, outside mcx accounting, and the snapshot
  accessors clone the active path per lookup (C iterates the list in place).
  A `thread_local!` cannot carry the `'mcx` lifetime; the owned-context shape
  for this is `mcx::McxOwned` (as the plancache pattern) or ownership by the
  future backend/session entry point per `docs/mctx-design.md` ("long-lived
  roots are owned by the eventual process entry point").
- **Suggested fix**: Move `NamespaceState` into an `McxOwned`-bundled context
  (or hang it off the session owner once one exists) so the search-path cache
  and base path are mcx-accounted with fallible allocation, and replace the
  per-call `active_search_path()` clones with in-place iteration under that
  owner.
- **Branch**: `port/backend-catalog-namespace`.

## backend-storage-lmgr-lmgr-seams: `LockGuard::drop` swallows unlock errors

- **Location**: `crates/backend-storage-lmgr-lmgr-seams/src/lib.rs`
  (`impl Drop for LockGuard`).
- **Description**: The guard's `Drop` (the abort path) delegates to the
  `unlock_*` seams and discards their `PgResult` — C's
  `elog(WARNING, ...)` on a lock-table inconsistency is lost on that path
  until elog/lmgr land. Explicit `release()` and the success-path `keep()`
  are unaffected.
- **Suggested fix**: When `TxnResources` (docs/query-lifecycle-raii.md) lands,
  move guards into the transaction owner whose teardown can surface the
  warning; until then, route the drop-path failure through elog once the
  error subsystem can be called from here.
- **Branch**: `port/backend-catalog-namespace`.

## xact state holds std collections, not PgVec in TopTransactionContext

- **Location**: `crates/backend-access-transam-xact/src/lib.rs`
  (`TransactionNode::{child_xids, name, retained_child_contexts}`,
  `XactState::{parallel_current_xids, unreported_xids, prepare_gid}`)
- **Description**: C allocates this transaction-lifetime data in
  `TopTransactionContext` (`childXids`, savepoint names and `prepareGID` via
  `MemoryContextStrdup`, `unreportedXids`); the port keeps it in plain std
  `Vec`/`String` inside the `thread_local!` `XactState`, even though the crate
  creates/resets the very `TopTransactionContext` it would live in. The state
  cannot borrow the context it also owns (self-referential thread_local), so
  the owned-handle plumbing the mcx design ultimately wants does not exist
  yet. Every allocating touch is fallible (`try_reserve`-style, OOM
  `PgError`), so the C failure surface is preserved; what is lost is the
  context accounting/reset coupling. Divergence is also declared in the
  crate's module doc.
- **Suggested fix**: once mcx grows owned-handle (non-borrowing) collection
  plumbing, move these collections into allocations charged to
  `top_transaction_context`.
- **Branch**: port/backend-access-transam-xact

## ParsedCommit/ParsedAbort carry owned alloc::Vec fields

- **Location**: `crates/types-wal/src/xact.rs` (`ParsedCommit`/`ParsedAbort`)
- **Description**: C's `xl_xact_parsed_commit` fields are pointers into the
  WAL record buffer — `ParseCommitRecord` allocates nothing — while the port's
  parsed shapes own `alloc::Vec` copies (`subxacts`, `xlocators`, `stats`,
  `msgs`, `twophase_gid`). The sibling `DecodedXLogRecord<'mcx>` in the same
  crate already demonstrates the borrowed/`PgVec<'mcx>` shape.
- **Suggested fix**: either borrow from the record data (lifetime parameter,
  matching C) or switch the fields to `PgVec<'mcx>` with
  `parse_commit_record`/`parse_abort_record` taking `Mcx<'mcx>`, matching
  `DecodedXLogRecord`.
- **Branch**: port/backend-access-transam-xact

## guc-tables consts.rs transcribed constants (drift hazard)

- **Location:** `crates/backend-utils-misc-guc-tables/src/consts.rs`.
- **Description:** The GUC tables' boot values/option values reference C
  macros and enums owned by mostly-unported subsystems (`BYTEA_OUTPUT_*`,
  `WAL_LEVEL_*`, `SYNCHRONOUS_COMMIT_*`, ...); consts.rs transcribes them
  with the values of the proven c2rust build. The elog-level constants
  already reference `types_error`; `DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE`
  stays transcribed even though dsm-core is ported, because the dependency
  edge runs owner -> guc-tables (dsm-core installs its option array and
  storage accessors into this crate's slots), so guc-tables cannot reference
  dsm-core's constant without a cargo cycle.
- **Fix:** As each owning unit lands, move its enum into the appropriate
  `types-*` crate (which guc-tables may depend on) and re-point the consts.rs
  entry at the real enum, deleting the transcription. `DSM_IMPL_*` /
  `DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE` should move to a `types-*` crate
  (e.g. types-storage) so both dsm-core and guc-tables can share them.

## pgstat with_shmem_*/with_snapshot_* callbacks are infallible

- **Location:** `crates/backend-utils-activity-pgstat-seams/src/lib.rs`
  (`with_shmem_archiver`/`with_snapshot_archiver` and the bgwriter /
  checkpointer pairs), consumers in
  `crates/backend-utils-activity-small/src/pgstat_{archiver,bgwriter,checkpointer}.rs`.
- **Description:** The callbacks are `&mut dyn FnMut(&mut T)` returning `()`
  — the AGENTS.md-sanctioned shape, no rule violated — but C bodies running
  under those pointers can `ereport` (`LWLockAcquire` inside the
  reset/snapshot callbacks), so consumers smuggle a `PgResult` out through a
  captured `let mut res` and return it after the call (e.g.
  `pgstat_archiver_reset_all_cb`).
- **Fix:** Switch the with-callbacks to a fallible shape, e.g.
  `with_shmem_archiver(f: &mut dyn FnMut(&mut PgStatShared_Archiver) ->
  PgResult<()>) -> PgResult<()>`, before more per-kind consumers land; the
  infallible report/fetch paths return `Ok(())` from their closures.

## backend-access-common-reloptions: transient parse working copies on the global allocator

- **Location:** `crates/backend-access-common-reloptions/src/lib.rs`
  (`parseRelOptions`/`parseRelOptionsInternal` `Vec<RelOptValue>`,
  `transformRelOptions`/`untransformRelOptions` `Vec<String>` working lists,
  `parse_one_reloption`'s `value.to_string()` for the seam args).
- **Description:** C pallocs the `relopt_value[]` working array and the
  `name=value` accumulator strings in `CurrentMemoryContext`; here they are
  query-lifetime `Vec`/`String` on the global allocator. The *result* byte
  buffer (`allocateReloptStruct`) and the `text[]` deconstruct/construct
  already go through `Mcx` (`mcx.oom`, the arrayfuncs seams). The working
  copies are dropped before the typed `RelOptStruct`/`AttributeOpts`/
  `TableSpaceOpts` is returned by value, so the divergence is bounded to
  transient state, but it is not fallible-`Mcx`-accounted as AGENTS.md
  prescribes. The option-*definition* tables (`relOpts`) are deliberately
  owned `String`/`Vec` per mcx-design decision 5 (backend-lifetime metadata,
  like C's `static const` tables) — those are not debt.
- **Suggested fix:** Thread the caller `Mcx` into the `RelOptValue` working
  list (store option values in `PgVec`) once `RelOptGen`/the definition table
  has an mcx-friendly representation; the `relopt_value` array is the only
  per-call allocation left on the global allocator.
- **Branch:** `port/backend-access-common-more`.

## backend-access-common-reloptions: relopt_kind / relopt_type as i32 aliases

- **Location:** `crates/backend-access-common-reloptions/src/lib.rs`
  (`relopt_kind`/`relopt_type`), `crates/types-reloptions/src/relopts.rs`
  (`StdRdOptIndexCleanup`/`ViewOptCheckOption`).
- **Description:** types.md rule 7 prefers Rust enums/newtypes over bare
  integer aliases. `relopt_kind` is kept as `i32` because C uses it as a
  `bits32` flag set (`RELOPT_KIND_HEAP | RELOPT_KIND_TOAST`, `kinds & kind`),
  not a discriminated enum — a newtype would obstruct the bit ops without
  adding safety. `relopt_type` and the two stored option-enum aliases match
  C's `int`-typed struct fields and the src-idiomatic representation.
- **Suggested fix:** If a later consumer crossing a seam needs the type
  safety, wrap `relopt_kind` in a `bitflags`-style newtype and
  `relopt_type`/the option enums in `#[repr(i32)]` enums verified against the
  headers; the values are already centralized here.
- **Branch:** `port/backend-access-common-more`.
## RelFileLocator duplicated between types-wal and types-storage

- **Location:** `crates/types-wal/src/wal.rs` (private fields + accessors,
  used by decoded WAL records) vs `crates/types-storage/src/storage.rs`
  (public fields, used by the smgr/bufmgr seams and types-xlog-records);
  converted by hand in
  `crates/backend-access-transam-xlogprefetcher/src/lib.rs::storage_locator`.
- **Description:** Two trimmed mirrors of `storage/relfilelocator.h`'s
  `RelFileLocator` exist at two layers (pre-existing before the
  xlogprefetcher port, which added the first cross-layer consumer). The
  prefetcher keys its filter table on the WAL-side type and converts at the
  smgr/bufmgr seam boundary.
- **Fix:** Unify on one definition (likely types-storage's, re-exported from
  types-wal) and update the wal-side `from_bytes`/accessor users.

## WONTFIX: two `FunctionCallInfoBaseData` homes (ABI carrier vs executor frame)

> UPDATE (fcinfo-on-step): `types-nodes` is **no longer `#![no_std]`** — it is now
> `std` and depends on `types-fmgr` so the `EEOP_FUNCEXPR` `Func` step OWNS the real
> `::fmgr::FunctionCallInfoBaseData` ABI carrier (`step_fcinfo`, C's
> `op->d.func.fcinfo_data` allocated once at `ExecInitFunc`, reused in place every
> tuple — see `function_call_invoke_step`). The two homes still coexist (the `'mcx`
> executor frame `nodes::fmgr::FunctionCallInfoBaseData<'mcx>` carries the
> OID/collation resolution metadata + arena `Node` links; the `std` ABI carrier is
> what the dispatch / builtins receive), so the unification arguments below stand —
> but the "`types-nodes` is `no_std`, so it cannot name the `std` ABI carrier" leg
> is OBSOLETE: it can, and the hot benchmark path now does, which is how the
> per-call frame-pool churn was removed.

- **Location:** `crates/types-fmgr/src/fmgr.rs`
  (`FunctionCallInfoBaseData`, lifetime-free) vs
  `crates/types-nodes/src/fmgr.rs`
  (`FunctionCallInfoBaseData<'mcx>`, the #296-widened executor frame).
  A third copy in `crates/pgrust-pg-ffi-fgram/src/fmgr.rs` is an inert
  c2rust FFI artifact (raw `c_void`/`c_char` ABI; consumed by NO hand-ported
  crate for its fmgr struct — the `-fgram` dependents use that crate only for
  grammar/parser vocabulary) and is NOT a live third model.
- **Verdict: WONTFIX — deliberate layering** (same class as the FmgrInfo
  dual-home #231 and the TypeCacheEntry dual-home #241). Verified on
  `origin/main` 6e6f90be.
- **Why they cannot be unified (concrete proof):**
  - **Different layers, different supertypes.** `types-fmgr` is the
    dependency-graph low-level fmgr-ABI carrier: it requires `std`
    (`Box<dyn Any>` internal lane, `Box<dyn ExpandedObject>`), is
    lifetime-free/`Clone`, embeds `types_fmgr::FmgrInfo` (typed `PGFunction`,
    `FnExpr`), and carries the Option-4 by-reference side channels
    (`ref_args`/`ref_result`/`internal_args`) that the boundary wrappers read.
    `types-nodes` is the `#![no_std]`+`alloc` executor knot crate (323
    dependents): its frame is `'mcx`-lifetime-bound, embeds the leaf
    `types_core::fmgr::FmgrInfo` (opaque `fn_addr: usize`), and carries
    arena/`Node` links (`context: Option<&'mcx Node>`,
    `resultinfo: ReturnSetInfo<'mcx>`). A single definition would have to be
    simultaneously `no_std` and `std`, simultaneously lifetime-free and
    `'mcx`-bound — it cannot.
  - **A cycle would be required either way.** `types-nodes` and `types-fmgr`
    do NOT depend on each other; both sit on the leaf `types-core`. Hosting one
    unified `'mcx`/`Node`-aware type in `types-fmgr` would force `types-fmgr`
    to depend on `types-nodes` (for `Node`/`ReturnSetInfo`), and `types-nodes`
    already pulls fmgr vocab from `types-core` — collapsing the layers creates a
    cycle and/or breaks `types-nodes`'s `no_std`.
  - **Zero unification pressure — they never meet.** The executor constructs
    the `types-nodes` frame (`backend-executor-execExpr` struct literals); the
    fmgr core constructs the `types-fmgr` carrier
    (`backend-utils-fmgr-core::init_fcinfo`,
    `FunctionCallInfoBaseData::new(...)`). No function takes one and
    returns/constructs the other, and no `From`/`Into` bridges them. The
    `function_call_invoke` seam
    (`backend-utils-fmgr-fmgr-seams::function_call_invoke`) is **value-based**
    (`fn_oid: Oid, collation: Oid, args: &[NullableDatum]) -> (DatumWord, bool)`)
    — neither struct ever crosses the crate boundary, so they are fully
    partitioned into disjoint crate forests.
- **Fix:** None planned. Keep both. Cross-reference comments at each
  definition prevent future audits from re-flagging this. Revisit only if a
  future single-tree node+ABI model (`'mcx` everywhere, `std`-only) ever
  removes the `no_std`/lifetime split — not on the roadmap.

## backend-access-transam-twophase: deferred mcx threading + lock-guard pair

- **What**: The 2PC port allocates its transient decode arrays and the
  `XactLog{Commit,Abort}RecordArgs` payloads (`decode_children`/`decode_rels`,
  `children.to_vec()`, `xids`, `get_prepared_transaction_list`) with owned
  `Vec`/`String` rather than threaded `Mcx<'mcx>`. C `palloc`s these in the
  current context. The `SaveState` builder growth (the largest, data-derived
  allocation) is already fallible via `try_reserve`; the remaining vectors are
  small, bounded by header counts.
- **Why deferred**: There is no allocation-context handle threaded into the 2PC
  state layer yet (the owned `TwoPhaseStateData` models shmem/backend-lifetime
  state, and the recovery/finish entry points are called from xact.c/xlog.c
  paths that do not yet hand down a `Ctx`/`Mcx`). Threading `Mcx<'mcx>` here
  forces a lifetime through the owned state model before its callers exist.
- **Suggested fix**: When the xact.c / xlogrecovery callers land with a `Ctx`,
  thread `Mcx<'mcx>` into `start_prepare`/`finish_prepared_transaction`/the
  recovery scans and switch the decode arrays + arg vectors to
  `vec_with_capacity_in`/`slice_in`.
- **Lock-guard pair**: `lock_twophase_state(bool)` / `unlock_twophase_state()`
  (`backend-storage-lmgr-lwlock-seams`) model `TwoPhaseStateLock` as an
  explicit acquire/release pair held across `?`, not a `Drop` guard, because
  the `lwlock-seams` lock/unlock decls still hand back `()` rather than an
  `LWLockGuard`. The inner work is wrapped in a closure whose result is bound
  before `unlock`, so the unlock still runs on the error path; convert to an
  `LWLockGuard` when the lwlock seams expose one. (The 2PC shmem substrate
  itself — `TwoPhaseShmemInit`/`TwoPhaseShmemSize` over the process-global
  `TwoPhaseState`, plus `with_twophase_state` and the 9 installed inward seams —
  has landed; this is no longer blocked on a deferred shmem owner.)
- **Exit hook**: `AtProcExit_Twophase` is registered through
  `backend-storage-ipc-seams::before_shmem_exit` but delegates to a
  thread_local cleanup slot (`set_proc_exit_cleanup`) because the locked-gxact
  bookkeeping is backend-private state the abort path owns; the hook is a no-op
  until the backend installs the cleanup. No release registry is introduced.
- **Branch**: `port/backend-access-transam-twophase`.

## xlog-driver — the XLogCtl shmem WAL-write / fsync driver

`backend-access-transam-xlog` (`access/transam/xlog.c`). The grounded core —
byte-pos<->LSN arithmetic, the segment/file-name codec, the checkpoint-distance
arithmetic, the `WalConfig` predicates, the WAL-retention horizon arithmetic,
the `CheckPoint` C-ABI image, the `CreateCheckPoint`/`CheckPointGuts`/
`CreateRestartPoint` state machine, and the `xlog_redo` opcode dispatch — is
ported 1:1. The WAL-engine DRIVER is deferred:

- `XLOGShmemSize`/`XLOGShmemInit`, `XLogWrite`/`AdvanceXLInsertBuffer`/
  `XLogFlush`/`XLogBackgroundFlush`/`XLogNeedsFlush`, `StartupXLOG`/
  `ShutdownXLOG`, `BootStrapXLOG`, the WAL segment-file lifecycle
  (`XLogFileInit`/`XLogFileOpen`), the `XLogCtl` shmem READ accessors
  (`GetRedoRecPtr`/`GetInsertRecPtr`/`GetFlushRecPtr`/… and the cluster
  identity/checksum readers), `RequestXLogSwitch`/`XLogRestorePoint`/
  `UpdateFullPageWrites`/`CheckXLogRemoved`, and the process-singleton
  `CreateCheckPoint`/`CreateRestartPoint`/`GetWALAvailability`/
  `SetConfigOptionInternal` entry points.

Each is an in-crate function that panics loudly with the `xlog-driver` tag.

**Why deferred:** the bodies operate the `XLogCtl` shared-memory region, the
open WAL segment files, and `global/pg_control` — they require the not-yet-ported
shared-memory / fd / spinlock substrate (`ShmemInitStruct`, `fd.c`, spinlocks).
Per the project rule, a code path may panic because a callee's crate isn't
ported yet; nothing is silently stubbed.

**Retire when:** the shmem / fd / spinlock substrate lands. Then build the real
`XLogCtl` shared state and the WAL-write/fsync driver bodies in this crate, and
the process-singleton `CreateCheckPoint`/`CreateRestartPoint` holders that drive
the already-ported `checkpoint::CreateCheckPoint` over an owned `CheckpointState`.

## xlog-checkpoint-deps — checkpoint cross-subsystem legs

`backend-access-transam-xlog::checkpoint::ext`. The checkpoint state machine is
ported; its calls into other subsystems (the WAL-insert-lock driver, bufmgr's
`CheckPointBuffers`, the SLRU/replication checkpoint callbacks, the
varsup/multixact/commit-ts/procarray snapshots, `sync.c`, slot invalidation,
`subtrans`, the walsummarizer/walreceiver/xlogrecovery reads, the recovery-command
runner, the control-file disk codec, and the `XLogInsert` engine for the two
checkpoint records) are deferred externals that panic with this tag.

**Retire when:** each owner subsystem lands; the external moves to a call through
that owner's `-seams` crate (or a direct dep where acyclic).

## xlog-checkpoint-record — runtime CreateCheckPoint (#157, LANDED)

`backend-access-transam-xlog::do_checkpoint`. The runtime checkpoint path
(`CreateCheckPoint` xlog.c:6951, `ShutdownXLOG` xlog.c:6664) is ported against the
real, already-ported substrate (the WAL-insert engine, the live `XLogCtl` shmem
accessors, the control-file disk codec, and the varsup/commit-ts/multixact owner
seams for the XID/CommitTs/Multi snapshots). It writes the durable
`XLOG_CHECKPOINT_{ONLINE,SHUTDOWN}` record (+ the `XLOG_CHECKPOINT_REDO`
redo-point marker for online checkpoints), updates `ControlFile`
(`checkPoint`/`checkPointCopy` + `ckptFullXid`), and the redo arm
(`redo::redo_checkpoint`/`RecoveryRestartPoint`) replays it. This supersedes the
graceful-degradation `create_checkpoint`/`shutdown_xlog` seams (which only flushed
buffers, wrote no record) and the owned-`CheckpointState`
`checkpoint::CreateCheckPoint`/`ext` path (still present for reference but no
longer the installed body). Verified: stats.sql `wal_bytes > before` deterministic
under contention; recovery TAP 45/45; clean shutdown/restart + crash recovery.

**Two documented single-node divergences (faithful to the crash-recovery
contract; only the hot-standby leg is affected):**
- `oldestActiveXid` is left `InvalidTransactionId` and the in-checkpoint
  `LogStandbySnapshot()` running-xacts snapshot is omitted. Both feed *archive
  recovery / hot-standby* reconstruction (`ProcArrayApplyRecoveryInfo`) only;
  single-node crash recovery never consults them, and bgwriter's periodic
  `LogStandbySnapshot` still emits `XLOG_RUNNING_XACTS`. Avoids a
  procarray/standby cross-dependency from the WAL engine.
- `CheckPointGuts` flushes CLOG + CommitTs + MultiXact + the buffer pool (the legs
  whose owners expose a CheckPoint seam) but not SUBTRANS / Predicate / the
  pre-buffer callbacks (RelationMap / ReplicationSlots / SnapBuild /
  LogicalRewriteHeap / ReplicationOrigin / TwoPhase) — those owners expose no
  CheckPoint seam yet. SUBTRANS is rebuilt from clog on recovery; the rest are not
  durability-critical for single-node crash recovery.

**Companion fixes landed with the keystone (pre-existing bugs the real checkpoint
exposed):** (1) `checkpointer::ReqShutdownXLOG` now `SetLatch(MyLatch)` (was
missing) — without it the SIGINT shutdown-checkpoint never woke the checkpointer
out of its `WaitLatch` (graceful-shutdown HANG). (2) `sync::process_sync_requests`
no longer holds the `SyncState` `RefCell` borrow across `AbsorbSyncRequests`
(re-entered `remember_sync_request` → double-borrow panic under checkpoint
contention). (3) `multixact::StartupMultiXact` resets `finishedStartup = false` at
the start of WAL recovery — pgrust keeps one shmem segment across a crash reinit
(C re-creates it), so a prior boot's `finishedStartup = true` would survive and
trip `SetMultiXactIdLimit`'s `Assert(!InRecovery)` in the checkpoint-redo arm.

**Retire the divergences when:** procarray/standby expose
`GetOldestActiveTransactionId`/`LogStandbySnapshot` seams reachable acyclically
from the WAL engine, and subtrans/predicate/relmapper/slot/snapbuild/origin/2pc
expose CheckPoint seams.

## xlog-redo-deps — xlog_redo per-arm work

`backend-access-transam-xlog::redo::ext`. The `xlog_redo` opcode dispatch is
ported. `XLOG_NEXTOID`, `XLOG_FPI{,_FOR_HINT}`, and the
`XLOG_CHECKPOINT_{ONLINE,SHUTDOWN}` / `XLOG_END_OF_RECOVERY` arms are now real
(`redo::redo_checkpoint` / `redo_end_of_recovery` / `RecoveryRestartPoint`, the
#157 keystone — they decode the `CheckPoint` image and advance the cluster
XID/OID/MultiXact counters + control file + `XLogCtl->ckptFullXid` through the
varsup/multixact owner seams). The remaining `xlog_redo_control_file_arm` consumers
(`XLOG_PARAMETER_CHANGE` / `XLOG_FPW_CHANGE`) stay deferred to the control-file
driver / xlogrecovery.

**Retire when:** `xlogrecovery` lands the `ControlFile->XLogCtl` parameter-change /
fpw-change durable state; those last two arms then call through its seam crate.
## slot_getallattrs seam over a payload-less TupleTableSlot

- **Location:** `crates/backend-executor-execTuples-seams/src/lib.rs`
  (`slot_getallattrs`), consumed by
  `crates/backend-replication-logical-proto/src/lib.rs`
  (`logicalrep_write_tuple`).
- **Description:** `types_nodes::TupleTableSlot` is still trimmed to its
  header bits (no descriptor/values payload), so the seam that models C's
  `slot_getallattrs(slot)` + `tts_values`/`tts_isnull` reads takes a slot the
  owner cannot yet locate data through — same acknowledged provisionality as
  the existing `exec_copy_slot` seam.
- **Suggested fix:** Re-sign the seam (or replace it with direct field reads)
  when the slot payload model lands with the execTuples port.
- **Branch:** port/backend-replication-logical-proto

## TD-PREPARE-1: opaque owner-pointer handles in commands/prepare

- **Location:** `crates/backend-commands-prepare`, with handle types in
  `crates/types-nodes/src/parsestmt.rs` and the new/extended owner `-seams`
  crates it calls.
- **Description:** prepare.c is a thin driver that threads several live
  parse/plan/exec values owned by unported subsystems —
  `CachedPlanSource`/`CachedPlan` (plancache), `Portal`/`MemoryContext`
  (portalmem), `ParamListInfo` (params), `ResourceOwner` (resowner),
  `EState`/`ExprState` (execUtils/execExpr), `DestReceiver`/`QueryCompletion`
  (caller). prepare.c never dereferences them, so they cross seams as opaque
  handle newtypes in `types_nodes::parsestmt` rather than as the real owner
  structs. This is inherited opacity (docs/types.md rule 6), not introduced —
  but it is debt: when an owner lands, the handle should resolve to its real
  type and the seam signatures re-signed.
- **Note:** `backend-executor-execUtils` is already ported with a concrete
  `ExecutorState`/`MemoryContext` model and no `es_param_list_info` field, so
  prepare's throwaway-EState lifecycle + parameter-evaluation seams
  (`create_executor_state`/`estate_set_param_list_info`/`free_executor_state`/
  `exec_prepare_expr_list`/`eval_exec_param_into_list`) are declared in
  `backend-executor-execExpr-seams` (the param evaluator) and panic until
  execExpr lands, instead of forcing an adapter onto execUtils' real signature.
- **Suggested fix:** As plancache/portalmem/params/resowner/execExpr land,
  replace the corresponding `parsestmt` handle with the real type (or the
  owner's canonical handle) and re-sign the seams; reconcile the EState path
  onto the real `ExecutorState` once `EStateData` carries `es_param_list_info`.
- **Branch:** port/backend-commands-prepare

## Two `JoinType`s + two `PathNode`s: planner arena vocab vs executor vocab

- **Unit:** backend-optimizer-path-joinpath
- **Description:** The join-path enumerator is built on the densely-aliased
  planner graph (`PlannerInfo`/`RelOptInfo`/`Path`/`RestrictInfo` shared by
  pointer identity, mutated by `add_path`). That model is unrepresentable in
  main's existing owned-tree `types_nodes::pathnodes::PathNode` (the executor's
  `ExecMaterializesOutput` capability tree) without `Rc`/`RefCell`. So this port
  introduces a **new** `types-pathnodes` crate with an arena/handle model
  (`RelId`/`PathId`/`RinfoId` indices into `PlannerInfo` arenas) and its own
  `PathNode`/`Path`. There are now two `PathNode` types (executor capability
  tree vs planner arena graph) and two `JoinType`s:
  - `types_nodes::jointype::JoinType` is a `#[repr(u32)]` enum trimmed to the
    join *executor* and is **missing `JOIN_RIGHT_SEMI`** — its discriminants
    `RIGHT_ANTI=6/UNIQUE_OUTER=7/UNIQUE_INNER=8` are shifted vs PG 18.3
    `nodes.h` (which has `RIGHT_SEMI=6`, `RIGHT_ANTI=7`, `UNIQUE_OUTER=8`,
    `UNIQUE_INNER=9`). joinpath needs the full, correct set, so `types-pathnodes`
    defines its own `JoinType` with verified `nodes.h` values.
  - Reusing main's enum is unsafe (wrong values, missing variant); fixing it is
    out of scope here and would break `nodeMergejoin` (which encodes 6/7/8).
- **Suggested fix:** When the foundational optimizer units land
  (`backend-optimizer-util-relnode` owns `RelOptInfo`/`PlannerInfo`,
  `-util-pathnode` owns the path constructors + arena `PathNode`,
  `-path-pathkeys`, `-util-restrictinfo`), ratify the `types-pathnodes` arena
  model as the single planner vocabulary and reconcile/repair the
  `types_nodes::jointype::JoinType` discriminants (add `JOIN_RIGHT_SEMI`, shift
  7→8/8→9) so executor and planner share one correct `JoinType`.
- **Branch:** port/backend-optimizer-path-joinpath
## ps_status PS_USE_CLOBBER_ARGV transmission not reproduced

- **Location:** `crates/backend-utils-misc-more/src/ps_status.rs`
  (`save_ps_display_args`, `os_set_proc_title`).
- **Description:** On Linux/macOS/Solaris PostgreSQL selects
  `PS_USE_CLOBBER_ARGV`, overwriting the original `argv`/`environ` memory to
  change the visible process title. That requires the raw `argv` pointers
  `main()` was handed (and `*_NSGetArgv()` fixups on macOS); this crate is not
  handed them, so `save_ps_display_args` only records the title-buffer bound
  and `os_set_proc_title` is a no-op on the CLOBBER_ARGV platforms (the BSD
  `setproctitle` path is real). The full buffer-management logic (prefix,
  activity, suffix, bounded truncation, `get_ps_display`) is complete and
  authoritative for callers; only the kernel-visible transmission is absent.
- **Suggested fix:** When the entry point (`backend-main-main` / the
  postmaster child launcher) threads the real `argv` region down, wire it into
  `save_ps_display_args` and implement the argv-clobber write in
  `os_set_proc_title`.
- **Branch:** port/backend-utils-misc-more


## Seam contract reconcile pending (declared-but-uninstalled seams on complete owners)

- **Location:** the `CONTRACT_RECONCILE_PENDING` allowlist in
  `crates/seams-init/src/lib.rs` (the `every_declared_seam_is_installed_by_its_owner`
  recurrence guard) and the listed owner crates' `init_seams()`.
- **Description:** The scoped seam-install regression guard fires for a declared
  seam only when its owner unit is `merged`/`audited` (COMPLETE), the seam is
  `::call`ed in non-test code, and it is not installed. The entries below clear
  the first three gates but CANNOT be installed by a bare `<fn>::set(ownerfn)`:
  either the owner's ported body has a **divergent signature** (extra `Mcx`
  param, a `PgResult`/`PgBox` wrapper the seam lacks or vice-versa, a C-style
  out-param vs a returned tuple, a baked-in constant, or a crate-local element
  type distinct from the seam's `types_*` type) or the backing body is
  **mis-homed** (lives in a different / not-yet-ported crate, or the seam's
  nominal owner is a split sibling that installs it elsewhere). `mirror-pg-and-panic`
  forbids altering a ported, audited body to force a `::set`, so these stay
  seam-and-panic and are tracked here + allowlisted (NOT a blanket skip — each
  line is named, and the guard re-asserts a retired line as stale debt).
- **Pending reconciles (owner :: seam — kind / mismatch):**
  - `backend-access-common-reloptions :: index_build_local_reloptions` — MISHOMED: owner has only `build_local_reloptions`; the fmgr-dispatch `index_opclass_options` tail `(FmgrInfo, Datum, bool)` body is not homed here.
  - `backend-access-heap-heaptoast :: heap_tuple_header_get_datum` — MISHOMED: `HeapTupleHeaderGetDatum` body lives in execTuples / types-tuple, not heaptoast.
  - `backend-access-table-tableam :: table_relation_set_new_filelocator` — UNPORTED: the `tableam.c` static-inline dispatch wrapper routes through `rd_tableam->relation_set_new_filelocator`, but neither the `TableAmRoutine` vtable slot nor its heap-AM body (`heapam_relation_set_new_filelocator`, `access/heap/heapam_handler.c`) is ported, so the complete tableam dispatch crate cannot install it. Consumed by `RelationSetNewRelfilenumber` (relcache.c) for `RELKIND_HAS_TABLE_AM` relations; seam-and-panic until heapam_handler lands the AM routine.
  - RETIRED: `get_table_am_routine` / `table_relation_toast_am` / `table_relation_needs_toast_table` / `table_parallelscan_reinitialize` — heapam_handler.c (core stage: scan/fetch/toast/parallelscan/filelocator vtable + tableamapi.c::GetTableAmRoutine) ported in `backend-access-heap-heapam-handler-core`, which installs all four provider-facing seams. (DML callbacks tuple_insert/delete/update/lock + the storage-creation leg of relation_set_new_filelocator delegate to `backend-access-heap-heapam-handler-dml-seams`, ported in the heapam-handler-dml stage.)
  - `backend-access-table-tableam :: table_beginscan` / `table_scan_getnextslot` / `table_scan_getnextslot_direction` — DIVERGENCE: the COPY/seqscan scan seams model the AM-owned scan state as an opaque `ScanToken(u64)`, but `tableam.c` was ported C-faithfully with the value-typed `TableScanDesc<'mcx>` descriptor. There is no `ScanToken`->descriptor registry (inventing one forges opacity), so the ported value-typed bodies cannot back these seams. (The matching VALUE-typed bitmap-scan `table_endscan` / `table_rescan` in `-bm-seams` DO back the ported bodies and ARE installed.) Pay down by unifying the COPY/seqscan scan model onto the value descriptor.
  - `backend-access-transam-parallel :: initializing_parallel_worker` — DIVERGENCE: only a `globals` field + a `set_*` rt-seam exist; no zero-arg `fn() -> bool` reader to install.
  - `backend-access-transam-xact :: define_savepoint` — DIVERGENCE: seam `(name: &str)` vs owner `DefineSavepoint(name: Option<&str>)`.
  - `backend-access-transam-xact :: set_xact_iso_level_read_committed` — DIVERGENCE: zero-arg read-committed setter vs owner's generic `SetXactIsoLevel(i32)`.
  - (xlog reconciled out 2026-06-13: CATALOG status corrected `merged`->`needs-decomp` (task #111). The whole WAL-insert/write/startup/buffer/shmem core is the unported port frontier, so its panic-stub seams are legitimate mirror-pg-and-panic — no longer tracked as pending-reconcile debt. Former entries: `{boot_strap_xlog, startup_xlog, xlog_shmem_init, xlog_shmem_size}`, `{data_checksums_enabled, enable_fsync, enable_hot_standby, wal_level, wal_sync_method, xlog_archive_command, xlog_archive_library}`, `recovery_in_progress`.)
  - `backend-access-transam-xlogprefetcher :: xlog_prefetch_shmem_size` — DIVERGENCE: owner `XLogPrefetchShmemSize() -> usize` vs seam `() -> PgResult<Size>`.
  - `backend-commands-functioncmds :: format_type_be` — MISHOMED: real body `format_type_be<'mcx>(Mcx, Oid) -> PgResult<PgString>` lives in `backend-utils-adt-format-type`.
  - `backend-commands-user :: is_reserved_name` — MISHOMED: `IsReservedName(&str) -> bool` lives in `backend-catalog-catalog`.
  - `backend-executor-execParallel :: {exec_init_parallel_plan_owned, exec_parallel_reinitialize_owned}` — BLOCKED (missing registry): the `_owned` seams take owned `&mut PlanStateNode` / `&mut EStateData` trees (called by nodeGatherMerge), but the owner's real `ExecInitParallelPlan` / `ExecParallelReinitialize` bodies operate entirely over handle-space (`PlanStateHandle` / `EStateHandle` opaque `usize` newtypes, threaded through every `sup::*::call`). No owned-tree→handle bridge exists (the seam doc comment names the prerequisite "parallel-planstate registry" explicitly). Installing requires either that registry or a crate-wide rewrite of the handle-based body onto owned trees — a contract redesign. Do not force-wire a fake handle.
  - `backend-executor-execPartition :: {exec_setup_partition_tuple_routing, exec_find_partition, exec_cleanup_tuple_routing}` — DIVERGENCE: owner uses crate-local `PartitionTupleRouting<'mcx>` (not the trimmed `types_nodes` type) + extra `Mcx` / bare-value-vs-`PgBox` returns.
  - `backend-executor-execMain :: {exec_set_param_plan_for_pending, link_subplan_planstate}` — UNPORTED (SubPlanState-reachability keystone): the PARAM_EXEC `execPlan` / `es_subplanstates` plumbing was RELOCATED off `execProcnode-seams` to `execMain-seams` (its real owner: the executor-owned `es_param_exec_vals` / `es_subplanstates`). The keystone `ParamExecData.execPlan` link landed (an `ExecPlanLink{plan_id}` index, NOT a registry), so the three field-level ops (`mark_param_execplan_pending`, `clear_param_execplan`, `param_execplan_pending`) are now INSTALLED by `backend-executor-execMain::init_seams`. The #166 execMain driver also now POPULATES `es_subplanstates` (the InitPlan loop `ExecInitNode`s each `plannedstmt->subplans` entry; ExecEndPlan tears them down). The remaining two seams stay blocked on TWO things execMain cannot resolve: (1) the *consumer* of `es_subplanstates` — `ExecInitSubPlan` (nodeSubplan) reached through a node's `Plan.initPlan` list — is gated behind the unported `Plan.initPlan` field (`execProcnode` `ExecInitNode`'s initPlan walk is a guard-and-panic; the trimmed `Plan` struct has no `initPlan`), so neither seam is reachable end-to-end; (2) `link_subplan_planstate` must set `sstate->planstate = list_nth(es_subplanstates, plan_id-1)`, but the owned model makes `SubPlanState.planstate` an *owning* `PgBox` (execAmi's `ExecReScan` walk consumes it as the owner), while `es_subplanstates` is ALSO an owning `PgVec<PgBox>` — two owners of one node. A faithful link needs `SubPlanState.planstate` re-modeled as a `plan_id` index into `es_subplanstates` (rippling types-nodes + execAmi + explain + execProcnode), and `exec_set_param_plan_for_pending` then resolves that index to re-enter `nodeSubplan::ExecSetParamPlan`. Seam-and-panic until that SubPlanState.planstate index re-model + `Plan.initPlan` land; then install both in `execMain::init_seams()` and DELETE this entry. (execMain is CATALOG `needs-decomp`, so the seam-install guard already exempts these; no allowlist row needed.)
  - `backend-executor-execTuples :: {slot_getattr, exec_force_store_heap_tuple, exec_force_store_minimal_tuple, exec_materialize_slot, exec_fetch_slot_minimal_tuple_copy}` — DIVERGENCE: seams use the owned-EState `SlotId`/`TupleTableSlot` form; owner bodies take `Mcx` + `&mut SlotData<'mcx>` (EState slot-pool indirection not yet modeled — see execTuples slot-model expansion).
  - `backend-executor-execTuples :: {exec_init_result_type_tl, execute_attr_map_slot_explicit, slot_getattr_by_id}` — MISHOMED: bodies belong to execJunk/tupconvert or the unwritten `SlotId` indirection, not execTuples. (`execute_attr_map_slot` (RriId form) is now installed in the owner: it resolves the map off `ri_ChildToRootMap` and delegates to the explicit transpose. `exec_store_generated_columns` was reconciled 2026-06-18: `ExecComputeStoredGenerated`'s per-attribute compute loop is now inlined in its real C home `backend-executor-nodeModifyTable` (lifecycle.rs), driving the slot-payload / `exec_eval_expr_switch_context` / `datum_copy_v` seams directly; the seam decl was removed.)
  - `backend-executor-execTuples :: slot_natts` — DIVERGENCE: `slot->tts_tupleDescriptor->natts` over an EState-pool `SlotId`; newly called by execExpr's `ExecInitWholeRowVar` (compiler-func-strict). The owned `TupleTableSlot`/`SlotId` slot-pool indirection (execTuples slot-model expansion, task #113) is not yet modeled, so the owner cannot install it — same class as the `slot_getattr*` entries above.
  - `backend-executor-nodeWorktablescan :: publish_wtparam_slot` — CONTRACT REDESIGN: the *deposit* end of the RecursiveUnion<->WorkTableScan cross-node aliasing channel. C `ExecInitRecursiveUnion` does `prmdata->value = PointerGetDatum(rustate)` — a live `RecursiveUnionState *` pointer stored into `es_param_exec_vals[wtParam]` and recovered by the descendant `WorkTableScan` via `resolve_rustate` (`castNode(RecursiveUnionState, DatumGetPointer(param->value))`). `ParamExecData.value` is the bare-word `Datum(usize)` (no pointer lane) and `WorkTableScanStateData.rustate` is an owned `Option<Box<RecursiveUnionStateData>>`, not an alias of the ancestor's `PgBox`. Installing the deposit faithfully needs the same unported datum-pointer/handle-arena machinery the recovery side (`resolve_rustate`, itself still seam-and-panic) requires — pay down jointly with that channel; do not force-wire/stub.
  - `backend-executor-execTuples :: {slot_getsomeattr, exec_store_first_datum, exec_store_minimal_tuple, exec_store_virtual_tuple, store_virtual_values, exec_copy_slot_minimal_tuple, exec_fetch_slot_minimal_tuple, exec_scan_slot_descriptor, replace_cur_tuple_from_slot, cur_tuple_getattr}` — UNPORTED (slot payload model): these read/write a slot's stored-tuple payload (`tts_values`/`tts_isnull`/`tts_nvalid`/`tts_tupleDescriptor`) or the producing slot's descriptor, addressed by an EState-pool `SlotId`/`&mut TupleTableSlot`. The `es_tupleTable` pool currently carries only the trimmed slot header (`tts_flags`/`tts_ops`/`tts_tid`/`tts_tableOid`); the per-attribute value arrays + descriptor live on the not-yet-woven `types_nodes::tuptable::SlotData` payload model (execTuples slot-model expansion, task #113, expanded `SlotData` but did not weave it into the pool). `exec_scan_slot_descriptor` is doubly-blocked: the scan slot's descriptor is dropped at `ExecInitScanTupleSlot` time. Same class as the `slot_getattr*` / store-fetch entries above — install when the payload model is woven into the slot pool. (The header-only `exec_alloc_table_slot` was reconciled + installed in this lane via a `desc.is_some()` adapter, mirroring `exec_init_scan_tuple_slot`.)
  - `backend-executor-execUtils :: exec_get_root_to_child_map` — DIVERGENCE: seam `(mcx, estate, RriId) -> Option<PgBox<AttrMap>>` vs owner `(&mut EStateData, RriId) -> Option<&TupleConversionMap>` (no Mcx, borrowed whole-map return).
  - `backend-executor-execUtils :: exec_get_updated_cols` — DIVERGENCE: param order + estate mutability differ (seam `mcx, &EStateData, rri`; owner `&mut EStateData, rri, mcx`).
  - `backend-executor-execUtils :: {exec_init_result_type_tl, exec_find_junk_attribute_in_tlist}` — MISHOMED: `ExecInitResultTypeTL` not bodied in either executor owner; `ExecFindJunkAttributeInTlist` is execJunk.c.
  - `backend-utils-fmgr-funcapi :: record_from_values` — UNPORTED (K1-gated, composite-Datum bridge): builds an anonymous-record `Datum` (`CreateTemplateTupleDesc` + per-col `TupleDescInitEntry` + `BlessTupleDesc` + `heap_form_tuple` + `HeapTupleGetDatum`). The terminal `HeapTupleGetDatum` step — turning a formed tuple into a composite record `Datum` — is unported workspace-wide: `types_tuple::Datum` is `TupleValue` (`ByVal`/`ByRef`), a scalar byte lane with no Composite/record arm, and no `FormedTuple`->record-Datum carrier exists. The owner cannot construct the seam's `Datum<'mcx>` return without fabricating a representation, so it stays seam-and-panic. Install once the FormedTuple→HeapTuple carrier bridge (task #161) lands. Consumed by `backend-utils-adt-misc2::admin::pg_stat_file`.
  - `backend-utils-fmgr-funcapi :: value_srf_unported` — UNPORTED (provider not landed): the value-per-call set-returning-function protocol (`SRF_IS_FIRSTCALL`/`SRF_FIRSTCALL_INIT`/`SRF_PERCALL_SETUP`/`SRF_RETURN_NEXT`/`SRF_RETURN_DONE` over a `FuncCallContext` with `multi_call_memory_ctx`/`user_fctx`). funcapi only models the materialize-mode tuplestore path; the value-SRF owner machinery is not yet ported, so the seam is declared genuinely-unported and panics loudly rather than silently degrading the SRF protocol (consumers — `pg_partition_tree`/`pg_partition_ancestors`/`pg_lock_status` in misc2 — wrap the call in `unreachable!`). Install when the value-SRF machinery lands.
  - `backend-nodes-core :: call_stmt_result_desc` — MISHOMED/UNPORTED (re-homed from `backend-nodes-nodeFuncs-seams` onto the `backend-nodes-core` owner so the guard can track it): `CallStmtResultDesc` (functioncmds.c) is keyed entirely by the unported call-expression node `CallStmt.funcexpr` (`FuncExpr.funcid`, opaque in the layered node tree) and folds into funcapi's `build_function_result_tupdesc_t` tupdesc spine + per-out-arg `exprType` re-typing. The node model and the funcapi tupdesc-builder callback seam do not exist yet, so the body stays seam-and-panic (mirror-pg-and-panic). Consumed by `backend-commands-functioncmds::call_stmt`.
  - `backend-nodes-core :: get_expr_result_type_node` — RETIRED. The non-`FuncExpr`/`OpExpr` arms of `get_expr_result_type` (funcapi.c) are all ported in place inside `backend-utils-fmgr-funcapi::result_type::get_expr_result_type`: the `RowExpr` arm builds the tupdesc directly, the generic arm runs `exprType`+`get_type_func_class`+`lookup_rowtype_tupdesc_copy`, and the RECORD-type-`Const` arm (reached only by EXPLAIN of SEARCH/CYCLE recursive CTEs) reads the composite Datum's `HeapTupleHeader` (`datum_typeid`/`datum_typmod`) and resolves via `lookup_rowtype_tupdesc_copy`. The seam declaration and its nodes-core no-op install are removed.
  - `backend-nodes-extensible :: {begin_custom_scan, create_custom_scan_state, end_custom_scan, estimate_dsm_custom_scan, exec_custom_scan, initialize_dsm_custom_scan, initialize_worker_custom_scan, mark_pos_custom_scan, reinitialize_dsm_custom_scan, rescan_custom_scan, restr_pos_custom_scan, shutdown_custom_scan}` — PROVIDER-UNPORTED: the CustomScan/CustomScanState provider callbacks (extensible.h `CustomScanMethods` / `CustomExecMethods`, dispatched by `nodeCustom.c` through `node->methods->X`) are supplied by a custom-scan-provider extension at registration time. There is no in-tree custom-scan provider — exactly the FDW-provider case (`backend-foreign-foreign` `begin_foreign_scan`/`iterate_foreign_scan`/… below), where `node->fdwroutine->X` dispatches through a runtime vtable with no ported owner. `backend-nodes-extensible` ports `extensible.c` and installs that file's registry side (`RegisterExtensibleNodeMethods`/`RegisterCustomScanMethods`/`GetExtensibleNodeMethods`/`GetCustomScanMethods`) in `init_seams()`, but there is no provider body to `::set` for these 12 callbacks, so they stay seam-and-panic (mirror-pg-and-panic) until a custom-scan provider is ported. Consumed by `backend-executor-nodeCustom`.
  - `backend-postmaster-autovacuum :: {am_autovacuum_launcher_process, am_autovacuum_worker_process}`, `backend-postmaster-bgworker :: am_background_worker_process`, `backend-replication-logical-slotsync :: am_logical_slot_sync_worker_process`, `backend-storage-lmgr-proc :: am_regular_backend_process` — MISHOMED: `MyBackendType`/miscadmin-derived predicates, not bodied in these crates.
  - `backend-utils-init-small :: init_process_globals` — PROVIDER-UNPORTED: `InitProcessGlobals()` is bodied in the unported `postmaster.c` keystone, not `globals.c` (the owner only holds the `MyStartTime[stamp]`/`MyProcPid` globals it would write). The body also needs two still-unported deps with no seam — `timestamptz_to_time_t` (timestamp.c) for `MyStartTime`, and the `pg_strong_random` entropy provider behind `pg_prng_strong_seed` for the per-backend global-PRNG reseed. Install once the strong-random provider + `timestamptz_to_time_t` seam land.
  - `backend-postmaster-interrupt :: {install_crash_exit_sigquit_handler, pqinitmask_set_blocksig}` — MISHOMED: signal-mask setup lives in `backend-libpq-pqsignal`, not interrupt.
  - `backend-replication-logical-origin :: set_replorigin_session_origin_lsn` — MISHOMED: no setter body; only the getter is implemented (the `_origin_lsn` field is never written yet).
  - `backend-storage-ipc :: {before_shmem_exit, on_exit_reset, check_on_shmem_exit_lists_are_empty, on_proc_exit, on_shmem_exit, proc_exit}` — MISHOMED (split-sibling owner): the `backend-storage-ipc-seams` bodies live in `backend-storage-ipc-dsm-core`, which installs `proc_exit`/`on_proc_exit`/`on_shmem_exit` (the guard infers owner `backend-storage-ipc` from the seams-crate name). `before_shmem_exit`/`on_exit_reset` await install in the dsm-core owner; `check_on_shmem_exit_lists_are_empty` returns `PgResult<()>` vs the seam's `()`.
  - `backend-storage-ipc-latch :: wait_latch_register_sync_request` — DIVERGENCE: zero-arg specialization vs owner's general `WaitLatch(Option<LatchHandle>, u32, i64, u32)`.
  - `backend-storage-ipc-pmsignal :: set_postmaster_death_watch_cloexec` — MISHOMED: the `fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFD, FD_CLOEXEC)` body lives in `miscinit.c` (called from `backend-utils-init-miscinit::process`), not in the pmsignal owner; the pmsignal crate has no body to `::set`.
  - `backend-tcop-backend-startup :: my_cancel_key` — MISHOMED: the `MyCancelKey` globals live in `backend-utils-init-small::globals`; backend-startup has no cancel-key body.
  - `backend-utils-adt-acl :: {has_bypassrls_privilege, object_ownercheck}` — MISHOMED: real owners are commands/user.c and catalog/aclchk.c (unported); the acl-seams decls are placeholder duplicates.
  - `backend-utils-fmgr-dfmgr :: load_file` — DIVERGENCE: owner `load_file(Mcx, &str, bool)` has an extra leading `Mcx`.
  - `backend-utils-fmgr-dfmgr :: {shmem_request_hook, shmem_request_hook_present, load_archive_module_init}` — MISHOMED: hook bodies are registrant-owned (driven from miscinit's `process_shmem_requests`); `load_archive_module_init` belongs to types-pgarch/pgarch.
  - `backend-utils-init-miscinit :: {initialize_session_user_id, process_session_preload_libraries}` — DIVERGENCE: owner bodies take an extra leading `Mcx`.
  - `backend-utils-init-miscinit :: {initialize_system_user, set_database_path_once}` — DIVERGENCE: owner bodies return `()` vs the seams' `PgResult<()>`.
  - `backend-utils-init-miscinit :: pg_usleep` — MISHOMED: real body is `port-pgsleep::pg_usleep`.
  - `backend-utils-init-miscinit :: setup_signal_handlers` — PROVIDER-UNPORTED: this is the slot-sync worker's `pqsignal(SIGHUP, SignalHandlerForConfigReload)`...`pqsignal(SIGCHLD, SIG_DFL)` block (`slotsync.c:1515-1522`); its handler bodies (`SignalHandlerForConfigReload`, `StatementCancelHandler`, `die`, `FloatExceptionHandler`, `procsignal_sigusr1_handler`) live in interrupt.c / postgres.c / procsignal.c, none of which is ported, so there is no real body to `::set`. The 8 sibling slot-sync bootstrap seams (`set_my_backend_type_slotsync`, `init_ps_display`, `init_process`, `base_init`, `initialize_timeouts`, `unblock_signals`, `init_postgres`, `check_for_interrupts`) ARE installed in miscinit's `init_seams()` by delegating to their now-ported owners (globals/ps_status/proc/postinit/timeout/pqsignal/postgres). Install `setup_signal_handlers` when interrupt.c/procsignal.c land.
  - `backend-utils-init-small :: {my_proc_port_application_name, my_proc_port_cmdline_options, my_proc_port_database_name, my_proc_port_guc_options, my_proc_port_user_name}` — DIVERGENCE: no per-field `Mcx`-copying accessor; only the `WithMyProcPort` closure access exists.
  - `backend-storage-lmgr-proc :: my_proc_latch` — DIVERGENCE: the seam hands out a `types_storage::latch::LatchHandle`, but proc's PGPROC models `procLatch` as an embedded `Latch`, not a registry-minted handle. The latch unit's handle registry (`allocate_latch`/`lookup_latch`) has no entry for an embedded PGPROC latch, so no valid `LatchHandle` can be minted over `with_my_proc` state without fabricating an opaque token the registry cannot resolve. Seam-and-panic until the latch unit exposes PGPROC procLatches as registry handles (same boundary as proc's `proc_latch(procno)` / `proc_latch_handle`).
  - `backend-storage-lmgr-proc :: {init_proc_global, initialize_fast_path_locks, proc_global_semas, proc_global_shmem_size}` — UNPORTED MACHINERY: these depend on `lock.c` fast-path lock slots and the ProcGlobal shmem sizing/semaphore arena that has not landed; the proc owner (audited) faithfully seam-and-panics them rather than fabricating a substrate. Install when lock-table fast-path + shmem arena wiring lands.
  - ~~`backend-storage-lmgr-proc :: {my_proc_xmin, set_my_proc_xmin, my_proc_xid, my_proc_vxid, my_proc_subxids, proc_subxids, store_top_xid_in_proc, store_subxid_in_proc}`~~ — RESOLVED (procarray landed, task #121): these are plain PGPROC field reads/writes over `proc_shmem`'s owned `MyProc`/`ProcGlobal` dense arrays (`with_my_proc`/`set_proc_array_xid`/`set_proc_array_subxid_state`), not arena-dependent. All 8 now have real bodies in `proc::inward_seams` and are installed in `init_seams()`. (The clog group-update PGPROC fields below stay deferred — they need clog.c's `TransactionGroupUpdateXidStatus` path.)
  - ~~`backend-storage-lmgr-proc :: {clog_group_first_read, clog_group_first_compare_exchange, clog_group_first_exchange, my_proc_clog_group_member, set_my_proc_clog_group_member, set_my_proc_clog_group_member_data, set_proc_clog_group_member, proc_clog_group_member_page, proc_clog_group_member_update, my_proc_clog_group_next, set_my_proc_clog_group_next, proc_clog_group_next, set_proc_clog_group_next}`~~ — RESOLVED (clog.c `TransactionGroupUpdateXidStatus` + procarray's `InitProcGlobal` arena landed): the `clog.c` group-commit batch (the `clogGroupFirst` CAS-linked list head + per-PGPROC `clogGroup{Member,Next,MemberXid,MemberPage,MemberXidStatus,MemberLsn}` fields). These are plain `ProcGlobal->clogGroupFirst` atomics (mirroring the procArrayGroupFirst CAS set) + PGPROC field reads/writes over `proc_shmem`'s owned `MyProc`/arena (`with_my_proc`/`with_proc_by_number`), not arena-dependent. All 13 now have real bodies in `proc::inward_seams` and are installed in `init_seams()`; clog.c's `TransactionGroupUpdateXidStatus` (the group-commit leader) consumes them.
  - `backend-utils-mmgr-portalmem :: {create_new_portal, portal_define_query, portal_get_portal_context, portal_set_visible}` — HANDLE-DIVERGENT (TD-PORTAL-HANDLE): PREPARE/EXECUTE's `-pre-seams` slice of portalmem.c is written against the parsestmt opaque handle newtypes (`PortalHandle(String)`, `MemoryContextHandle(u64)`, consumed by `backend-commands-prepare`), while the owner's real ported bodies (`CreateNewPortal`/`PortalDefineQuery`/`PortalSetVisible`/`GetPortalContext`) operate on the value-typed `types_portal::Portal` (`Rc<RefCell<PortalData>>`) and a real `MemoryContext`. Installing the handle seams would need a `PortalHandle`/`MemoryContextHandle` -> value registry (a forbidden token-registry hack — `opacity-inherited-never-introduced`) or migrating PREPARE/EXECUTE off the opaque parsestmt handles onto owned portal values (the K1 de-handle work, tasks #159/#169). Seam-and-panic until then; install + delete when PREPARE/EXECUTE is de-handled. (Tracked alongside the existing TD-PREPARE-1 entry.)
  - `backend-utils-mmgr-portalmem :: {copy_param_list_into_portal, copy_tup_desc_into_hold_context, portal_define_query_select}` — UNPORTED (TD-PORTAL-COPYIN): the deep-copy-into-portal-context seams (consumed by `backend-commands-portalcmds`) copy foreign objects — param lists (`copyParamList`), tuple descriptors (`CreateTupleDescCopy`), planned statements (`copyObject`) — into the portal's `'static`-lifetime owned arenas (the portal/hold `MemoryContext` that outlives the source transaction). That copy infrastructure lands with the tuplestore/tupdesc copy owners; the owner crate seam-and-panics them (matching the pre-port `todo` state) rather than wrongly stubbing a shallow alias. Install + delete when the copy-into-portal-context owners land.
  - ~~`backend-nodes-copyfuncs :: {copy_query_list, copy_plan_list, copy_raw_stmt, copy_analyzed_query, copy_expr, query_list_elements, plan_list_elements, extract_query_dependencies, expression_planner_with_deps}`~~ — RESOLVED (#159 STEP C plancache de-handle): plancache no longer stores opaque plancache token newtypes; `CachedPlanSourceData`/`CachedPlanData`/`CachedExpressionData` now own the `Query`/`PlannedStmt`/`RawStmt`/`Expr` value trees in private `MemoryContext`s (clone_in + `'static` drop-order, the portalmem pattern), and the dependency/planner-deps work crosses the value seams `extract_query_dependencies_value` / `expression_planner_with_deps_value`. These 9 -pc-seams handle forms are no longer called and their allowlist entries were deleted. `backend-nodes-copyfuncs :: list_member_oid` STAYS (a separate list.c primitive, still declared+called+uninstalled, not part of the de-handle slice).
  - `backend-commands-trigger :: {renametrig}` — F1-PARTIAL / HANDLE-DIVERGENT (TD-TRIGGER-F1): trigger.c's AFTER-row firing (`AfterTriggerExecute` materializing the OLD/NEW slot payloads onto the per-call side-channel as a re-fetched on-page `FormedTuple` + the trigger relation's descriptor) now lands, so the slot-value/live-relation/snapshot-self accessors (`tg_relation`, `slot_tid`, `slot_attisnull`, `slot_is_current_xact_tuple`, `slot_getattr`, `pk_datum_image_eq`, `tg_relation_tuple_satisfies_snapshot_self`) are now INSTALLED + DELETED from the allowlist (the scalar `TriggerData`/`Trigger` accessors were installed earlier). `RemoveTriggerById` (the pg_trigger catalog-delete DDL leg) and `get_trigger_oid` (the pg_trigger by-(relid,name) scan) are now ported + installed (remove.rs / firing.rs), so they are deleted from the allowlist. `tg_trigtuple`/`tg_newtuple` (the OLD/NEW HeapTuple accessors) are now INSTALLED off the current-trigger side-channel — read by `plpgsql_exec_trigger` to populate the NEW/OLD expanded records — so they are DELETED from the allowlist. `renametrig` (the ALTER TRIGGER ... RENAME TO catalog-write DDL leg, including `renametrig_internal`/`renametrig_partition` partition recursion and the `RangeVarCallbackForRenameTrigger` lock-acquire callback) is now ported + installed (rename.rs), so it too is DELETED from the allowlist — the whole `backend-commands-trigger` allowlist block is now empty. The accessor seams are keyed by the opaque foreign handles `types_ri_triggers::{TriggerDataRef, TriggerRef, TupleTableSlotRef}` (u64 newtypes); the firing path resolves them to the side-channel payload. NOTE: the RI enforcement *queries* themselves (RI_FKey_check/noaction/etc) bottom out at `SPI_prepare` (raw_parser + analyze + one-shot plancache, the SPI-decomp keystone), a separate campaign. Surfaced by AUDIT-FIX #345.
  - `backend-access-index-genam :: {build_index_value_description}` — UNPORTED (TD-GENAM-RELCACHE-SCANS): the genam unit ported genam.c's `systable_*` primitive engine (begin/getnext/endscan, installed) AND the relcache catalog scan-and-decode helpers (ScanPgRelation's `scan_pg_class`, RelationBuildTupleDesc's `scan_pg_attribute`, RelationGetIndexList's `relcache_scan_pg_index`, GetStatExtList's `relcache_scan_pg_statistic_ext`, GetFKeyList's `relcache_scan_pg_constraint_fkeys`, GetExclusionInfo's `relcache_exclusion_info`, AttrDefaultFetch's `scan_pg_attrdef`, CheckNNConstraintFetch's `scan_pg_constraint_nncheck`, RelationBuildRuleLock's `relcache_scan_pg_rewrite`) — all bodied + installed in `src/decode.rs` (`table_open` + `systable_beginscan`/`getnext` + per-row `heap_deform_tuple` GETSTRUCT decode), so their allowlist entries were removed. `systable_inplace_update` (the buffer-locking retry + `heap_inplace_update_and_unlock` loop) is likewise bodied + installed. Only `build_index_value_description` (per-key out-function + ACL-visibility render) remains a genam.c function not yet bodied; install + delete when the genam unit ports its render body. Surfaced by AUDIT-FIX #345.
  - `backend-utils-cache-relcache :: {relation_fdwroutine, set_relation_fdwroutine}` — UNPORTED (TD-RELCACHE-FDWROUTINE): these read/write the relcache entry's `rd_fdwroutine` cache slot (foreign.c `GetFdwRoutineForRelation` memoizes the resolved `FdwRoutine` there), but `types_rel::RelationData` does not model an `rd_fdwroutine` field yet, so the relcache owner has no slot to read/cache into. (The other 6 relcache seams the #345 guard fix surfaced — `critical_relcaches_built`, `critical_shared_relcaches_built`, `assert_could_get_relation`, `rd_indcollation`, `index_getprocid`, `relation_set_new_relfilenumber` — WERE installed in this lane against existing owned state.) Install + delete when the relcache entry gains the `rd_fdwroutine` cache slot. Surfaced by AUDIT-FIX #345.
  - `backend-utils-cache-relcache :: {relation_get_index_expressions, relation_get_index_predicate, relation_get_exclusion_info}` — UNPORTED (TD-RELCACHE-INDEX-NODETREE): `BuildIndexInfo` (#334, catalog/index.c) calls `RelationGetIndexExpressions` / `RelationGetIndexPredicate` (and, for exclusion indexes, `RelationGetExclusionInfo`) unconditionally, mirroring the C `makeIndexInfo(... RelationGetIndexExpressions(index), RelationGetIndexPredicate(index) ...)`. The relcache owner's bodies delegate the `pg_index.indexprs`/`indpred` `stringToNode` + `eval_const_expressions` + `fix_opfuncids` node-tree transform to `nodexform_seam::index_{expressions,predicate}` (the node-tree string reader), which is unported — so the relcache owner cannot install them and they loud-panic (mirror-PG-and-panic) when reached. The live `BuildIndexInfo` consumers (bootstrap catalogs, brin, amcheck) index simple columns, where the C returns NIL without decoding, so the panic only fires on a real expression / predicate / exclusion index. Install + delete these three when the node-tree decode (`stringToNode`) lands. Surfaced by #334.
  - `backend-utils-cache-relcache :: relation_get_dummy_index_expressions` — UNPORTED (TD-RELCACHE-INDEX-NODETREE, cont.): `BuildDummyIndexInfo` (#334, catalog/index.c) reads its expression list from `RelationGetDummyIndexExpressions` — the same `pg_index.indexprs` `stringToNode` node-tree decode as `RelationGetIndexExpressions`, then it replaces every expression leaf with a null `Const` of the right type/typmod/collation (so no user code runs). It rides on the SAME unported node-tree string reader, so the relcache owner cannot install it and it loud-panics (mirror-PG-and-panic) until `stringToNode` lands. The live consumer (TRUNCATE of an index) only reaches the decode on an expression index; simple-column indexes return NIL. Install + delete alongside the three entries above. Surfaced by #334.
  - `backend-catalog-indexing :: append_attribute_tuples` — UNPORTED (TD-INDEXING-APPEND-ATTRIBUTE-TUPLES): `AppendAttributeTuples` (#334, catalog/index.c) inserts one `pg_attribute` row per index column (`InsertPgAttributeTuples(pg_attribute, indexTupDesc, InvalidOid, attrs_extra, indstate)`) after `InitializeAttributeOids` (scribble the new index's OID onto its stored descriptor's `attrelid`s). The catalog-indexing owner owns pg_attribute writes but has not yet ported `InsertPgAttributeTuples` (the `heap_form_tuple` over `Form_pg_attribute` + `CatalogTuplesMultiInsertWithInfo` path) nor the descriptor-mutation entry point, so it cannot install this and the seam loud-panics (mirror-PG-and-panic). Reached only from `index_create`, which is itself uninstalled (catalog-write driver substrate unported). Install + delete when catalog-indexing ports `InsertPgAttributeTuples`. Surfaced by #334.
  - `backend-utils-cache-inval :: cache_invalidate_heap_tuple` — UNPORTED / DIVERGENT (TD-INVAL-OID-REFETCH): the `(classId, objectId)` reduction of `CacheInvalidateHeapTuple(rel, tuple, NULL)` the typecmds ALTER DOMAIN paths call. The inval owner has the shared engine (`cache_invalidate_heap_tuple_common(relation, tuple, ...)`) but not the OID re-fetch wrapper (open the catalog by `class_id` + syscache-fetch by `object_id`, then run the common path); the signature diverges (OID pair vs `&RelationData` + `&HeapTupleData`), so no bare `::set` of the common body fits. Install + delete when the inval owner adds the OID-keyed re-fetch wrapper. Surfaced by AUDIT-FIX #345.
  - `backend-utils-misc-guc-file :: {at_eoxact_guc, new_guc_nest_level, set_config_with_handle}` — MISHOMED: belong to guc.c (a separate unit), not the guc-file crate.
  - `backend-utils-misc-guc-file :: guc_check_errdetail` — MISHOMED: `GUC_check_errdetail()` writes guc.c's per-call check-hook error globals (a separate guc.c unit), not bodied in the guc-file crate. Newly surfaced by `backend-access-transam-commit-ts`'s `check_commit_ts_buffers` hook, which path-qualifies the call (`backend_utils_misc_guc_file_seams::guc_check_errdetail::call()`) where prior callers (`namespace`, `user`) used an aliased import the guard could not attribute.
- **Fix:** Reconcile each contract when the divergent owner is reworked or the
  mis-homed body lands — adapt the seam decl (or the owner sig), install it,
  and DELETE the entry from `CONTRACT_RECONCILE_PENDING` so the guard
  re-asserts the install. A retired-but-still-listed entry is surfaced by the
  guard as "stale CONTRACT_RECONCILE_PENDING".
- **Branch:** assemble/seam-wiring-guard

## execExprInterp `exec_eval_expr_switch_context` seam: shared-vs-mut divergence

- **Location**: `crates/backend-executor-execExprInterp-seams/src/lib.rs`
  (`exec_eval_expr_switch_context`), owned by
  `crates/backend-executor-execExprInterp` (`dispatch::ExecInterpExprStillValid`),
  consumed by `crates/backend-executor-execExpr/src/execExpr_core.rs`.
- **Description**: The seam declares `state: &ExprState` (mirroring the C
  `ExecEvalExprSwitchContext` macro, which reads `state->evalfunc` through a
  pointer). The owned interpreter dispatch entry `ExecInterpExprStillValid`
  needs `&mut ExprState`: the first-call `CheckExprStillValid` path and the
  selected `ExecJust*` / `ExecInterpExpr` evalfuncs mutate per-eval scratch
  (`InterpEvalFunc::call` takes `&mut`). The shared seam ref cannot be installed
  against the `&mut` entry, so this one seam stays seam-and-panic; the sibling
  `exec_ready_interpreted_expr` (`&mut` in both) IS installed by
  `init_seams()`. Tracked in `CONTRACT_RECONCILE_PENDING`
  (`backend_executor_execExprInterp`, `exec_eval_expr_switch_context`).
- **Suggested fix**: in the seam-contract-reconcile lane, decide the canonical
  receiver — either thread `&mut ExprState` through the seam (and the
  execExpr_core `ExecEvalExprSwitchContext` wrappers) to match the owned model,
  or move the per-eval mutable scratch behind interior mutability so the entry
  can take `&ExprState`. Then install the seam and DELETE the
  `CONTRACT_RECONCILE_PENDING` entry so the guard re-asserts the install.
- **Branch**: assemble/backend-executor-execExprInterp

## indexam scan seams: node-driven decls vs C-faithful tableam owner model

- **Location**: `crates/backend-access-index-indexam-seams/src/lib.rs` (all 16
  scan seams), owned by `crates/backend-access-index-indexam`, consumed by
  `crates/backend-executor-nodeIndexonlyscan`,
  `crates/backend-executor-nodeBitmapIndexscan`, and
  `crates/backend-access-nbtree-nbtree`.
- **Seams**: `index_beginscan`, `index_beginscan_bitmap`,
  `index_beginscan_parallel`, `index_rescan`, `index_rescan_bis`,
  `index_getnext_tid`, `index_fetch_heap`, `index_endscan`, `index_markpos`,
  `index_restrpos`, `index_getbitmap`, `index_parallelscan_estimate`,
  `index_parallelscan_initialize`, `index_parallelrescan`,
  `bt_resolve_parallel_scan`, `index_scan_resolve_shared_info`.
- **Description**: The seam decls codify a node-driven scan model — the scan
  descriptor is `types_nodes::nodeindexonlyscan::IndexScanDescData` /
  `ParallelIndexScanDescData` (PgBox handles, C-faithful field names like
  `heapRelation`/`numberOfKeys`/`keyData: PgVec`), snapshots are
  `Option<Rc<SnapshotData>>`, the heap fetch is `SlotId` + `&mut EStateData`,
  begins are `Mcx`-first, and rescan takes the node (`IndexOnlyScanState` /
  `BitmapIndexScanState`) so the AM reads `ioss_ScanKeys`/`biss_ScanKeys`. The
  live consumers call the seams with exactly those types
  (`node.ioss_ScanDesc: Option<types_nodes::IndexScanDesc>`).
  The owner crate, however, faithfully ported indexam.c against the
  **C-faithful `types_tableam::relscan::IndexScanDescData`** model: snake-case
  fields (`heap_relation`/`number_of_keys`), by-value `SnapshotData`, scan-key
  slices passed explicitly, `&mut TupleTableSlot` heap fetch, and dispatch
  through the `IndexAmRoutine` vtable typed on that struct. nbtree adds a third
  view (`NbtScan`). The seam-decl `IndexScanDescData` and the owner-impl
  `IndexScanDescData` are two **independent structs in different crates** that
  were never reconciled, so no thin forwarding adapter can be written: an
  adapter would have to marshal field-by-field between two distinct scan-desc
  representations (including the AM-private `opaque` state) and synthesize the
  EState slot-pool / DSM-pointer machinery the node model assumes.
- **Suggested fix**: in the index-AM model-unification lane, pick one canonical
  `IndexScanDescData` (and `ParallelIndexScanDescData`) shared by
  indexam/genam/nodes/nbtree, retype the owner's dispatch + the `IndexAmRoutine`
  vtable onto it, and re-express the heap-fetch on the EState slot pool. Then
  install all 16 seams in the owner's `init_seams()` and DELETE the
  `CONTRACT_RECONCILE_PENDING` entries so the guard re-asserts the install.
- **Branch**: fix/sw2-backend-access-index-indexam

## backend-foreign-foreign-seams bundles 40 seams owned by two still-unported domains

- **Location:** `crates/backend-foreign-foreign-seams/src/lib.rs` (the
  catalog-DML / options / IMPORT seams + the FDW-provider-callback seams),
  name-attributed to owner `backend-foreign-foreign`
  (`crates/backend-foreign-foreign`). Consumed by
  `crates/backend-commands-foreigncmds` and
  `crates/backend-executor-nodeForeignscan`. Tracked in
  `crates/seams-init/src/lib.rs` `CONTRACT_RECONCILE_PENDING` (40 entries).
- **Description:** `backend-foreign-foreign` ports `foreign/foreign.c` and
  installs that file's READ accessors (`get_foreign_data_wrapper[_by_name]`,
  `get_foreign_server_by_name`, `get_foreign_{server,data_wrapper}_oid`,
  `is_importable_foreign_table`, `mapping_user_name`) plus FDW-routine
  resolution (`get_fdw_routine_{for_relation,by_server_id}`) — 9 seams, all
  installed in its `init_seams()`. The remaining 40 declared seams in the same
  `-seams` bundle are NOT `foreign.c` functions; the guard attributes them to
  this owner only by the `-seams` crate's name. They fall in two unported
  domains, so installing them here would require faking
  (`opacity-inherited-never-introduced`):
  1. **pg_foreign_* catalog DML + options decode + IMPORT + dynamic validator
     dispatch** — `insert_{fdw,server,usermapping,foreign_table}`,
     `update_{fdw,server,usermapping}`, `{fdw,server}_set_owner`,
     `{fdw,server}_lookup_by_name`, `{fdw,server}_owner_row_by_{name,oid}`,
     `{fdw,server,usermapping}_options`, `usermapping_oid`, `validate_options`,
     `fdw_import_foreign_schema`, `import_classify_raw_stmt`,
     `import_set_schemaname`. These are `commands/foreigncmds.c` machinery
     (`heap_form_tuple` + `CatalogTupleInsert`/`Update`, `SearchSysCacheCopy1`,
     `GetNewOidWithIndex`, `SysCacheGetAttr` decode, `aclnewowner`,
     `OidFunctionCall2(fdwvalidator, …)`, and `pg_parse_query` RawStmt
     projection) — they need the pg_foreign_* catalog-write substrate (no
     writable pg_foreign_* relation is reachable from this owner) and a dynamic
     fmgr validator dispatch / unported parser RawStmt node. `foreigncmds`
     (merged) is a CONSUMER of these seams, not their installer.
  2. **FDW-provider callbacks** — `begin_foreign_scan`, `begin_direct_modify`,
     `iterate_foreign_scan`, `iterate_direct_modify`, `rescan_foreign_scan`,
     `end_foreign_scan`, `end_direct_modify`, `recheck_foreign_scan`,
     `stamp_scan_slot_tableoid`, the parallel-DSM set
     (`estimate_dsm_foreign_scan`, `initialize_dsm_foreign_scan`,
     `reinitialize_dsm_foreign_scan`, `initialize_worker_foreign_scan`,
     `shutdown_foreign_scan`), and the async set (`foreign_async_request`,
     `foreign_async_configure_wait`, `foreign_async_notify`). In C these
     dispatch through `node->fdwroutine->X` — a runtime FDW vtable. No FDW
     provider (postgres_fdw / a contrib FDW) is ported, so there is no vtable to
     install. `nodeForeignscan` (merged) `::call`s them but they can only be
     satisfied once an FDW provider lands.
  Because `backend-foreign-foreign`'s CATALOG status is `audited` (complete) and
  all 40 are `::call`ed in non-test code, the hardened guard treats them as
  latent runtime panics; they are recorded here as accepted debt rather than
  force-wired or stubbed.
- **Fix:** Pay these down in the owning domains, not here. (1) When the
  pg_foreign_* catalog-write path lands (a `foreigncmds`/catalog-access owner
  with `heap_form_tuple` + `CatalogTupleInsert`/`Update` + the syscache row
  decoders + dynamic validator dispatch), re-home those catalog-DML/options/
  IMPORT decls to that owner's `-seams` crate, install them, and delete their
  `CONTRACT_RECONCILE_PENDING` entries. (2) When an FDW provider is ported,
  re-home the provider-callback decls to `backend-executor-nodeForeignscan`'s
  (or the provider's) `-seams` crate with a real `node->fdwroutine` vtable,
  install, and delete those entries. The guard then re-asserts each install.
- **Branch:** fix/sw2-backend-foreign-foreign
## bgworker `background_worker_handle_from_token` seam: token producer unported

- **Location**: `crates/backend-postmaster-bgworker-seams/src/lib.rs`
  (`background_worker_handle_from_token`), owned by
  `crates/backend-postmaster-bgworker`, consumed by
  `crates/backend-storage-ipc-shm-mq/src/lib.rs` (`shm_mq_set_handle`, via a
  function-local `use`).
- **Description**: The seam converts an execParallel
  `types_execparallel::BackgroundWorkerHandle(usize)` *token* back into the
  owner's real `types_bgworker::BackgroundWorkerHandle { slot, generation }`.
  C never needs this conversion — the leader holds a raw
  `BackgroundWorkerHandle *` directly. In this repo the token is minted only by
  the parallel runtime seam
  `backend_access_transam_parallel_rt_seams::register_dynamic_background_worker`
  (`-> BgwHandle`), which the merged `backend-access-transam-parallel` crate
  *calls* as an OUTWARD harness seam but which NO crate installs (the parallel
  runtime / postmaster launch leg is unported). Because the producing side that
  would populate a token->handle registry does not exist, the bgworker owner has
  no faithful way to decode an arbitrary token id to a real `{slot, generation}`
  handle. The owner's own `register_dynamic_background_worker` inward seam
  returns the real handle *value* directly (not a token), so it never mints a
  token to register either. Installing this seam therefore requires the
  unported parallel-runtime token-minting machinery, not pure wiring.
- **Suggested fix**: when the parallel-runtime / postmaster bgworker launch leg
  is ported and installs
  `backend_access_transam_parallel_rt_seams::register_dynamic_background_worker`,
  have that producer mint each token through a bgworker-owned token registry
  keyed by the real `{slot, generation}` handle; then implement
  `background_worker_handle_from_token` as a registry lookup in the owner's
  `init_seams()` and DELETE the `CONTRACT_RECONCILE_PENDING`
  (`backend_postmaster_bgworker`, `background_worker_handle_from_token`) entry so
  the guard re-asserts the install.
- **Branch**: fix/sw2-backend-postmaster-bgworker
## TD-TUPDESC-HANDLE: handle-vs-value divergence for plancache tupdesc seams

**RESOLVED (#159 STEP C plancache de-handle):** plancache now owns `TupleDescData`
values in a private `MemoryContext` (clone_in via the value `create_tuple_desc_copy`
seam; freed by dropping the context). The handle-based `free_tuple_desc` pc-seam is no
longer called and its allowlist entry was deleted.

- **Location**: seams `create_tuple_desc_copy` / `free_tuple_desc` /
  `equal_row_types` declared in
  `crates/backend-access-common-tupdesc-pc-seams`, owned by
  `crates/backend-access-common-tupdesc`
  (`CreateTupleDescCopy` / `FreeTupleDesc` / `equalRowTypes`), consumed by
  `crates/backend-utils-cache-plancache/src/lib.rs`.
- **Description**: The `-pc-seams` decls are HANDLE-based: they take/return
  `types_plancache::TupleDescHandle`, an opaque `u64` token with NO backing
  registry — plancache only threads the token through (it originates from the
  equally opaque `exec_clean_type_from_tl` / `utility_tuple_descriptor` seams
  and is never materialized into a real descriptor). The owner's real bodies and
  its installed `-seams` are VALUE-based over `&TupleDescData`/`PgBox<...>`. The
  owner therefore cannot `::set` the handle seams from its existing impls: there
  is no way to convert a bare `u64` into a `&TupleDescData`. The name-keyed
  seam-wiring guard flags only `free_tuple_desc` (the other two handle-seam names
  collide with the installed value-seam names so it sees them as satisfied), but
  all three are equally uninstalled at runtime — same blocker. Tracked in
  `CONTRACT_RECONCILE_PENDING` (`backend_access_common_tupdesc`,
  `free_tuple_desc`).
- **Why deferred**: Installing requires either (a) a
  `TupleDescHandle -> TupleDescData` registry/arena — substantial unported
  machinery, and a token-registry hack of the kind opacity-inherited-never-introduced
  forbids — or (b) migrating plancache's entire result-desc path off opaque
  handles onto value `TupleDescData`, a contract redesign rippling through the
  pquery/utility/analyze handle seams. Neither is a force-wire or stub.
- **Suggested fix**: in the contract-reconcile lane, choose the canonical
  tupdesc boundary model (value `TupleDescData` end-to-end, or a real owned
  descriptor handle with a registry), migrate plancache + the pquery/utility
  result-desc producers to it, fold `-pc-seams` into the value `-seams`, install,
  and DELETE this allowlist entry so the guard re-asserts the install.
- **Branch**: fix/sw2-backend-access-common-tupdesc
## namespace search-path matcher seams: handle/CtxId vs value/Mcx contract divergence

- **Location**: `crates/backend-catalog-namespace-pc-seams/src/lib.rs`
  (`get_search_path_matcher`, `copy_search_path_matcher`,
  `search_path_matches_current_environment`), owned by
  `crates/backend-catalog-namespace`, consumed by
  `crates/backend-utils-cache-plancache`.
- **Description**: The pc-seams declare a handle-shaped contract
  (`get_search_path_matcher(context: CtxId) -> SearchPathMatcherHandle`,
  `copy_search_path_matcher(SearchPathMatcherHandle) -> SearchPathMatcherHandle`,
  `search_path_matches_current_environment(SearchPathMatcherHandle) -> bool`)
  because the matcher's storage lives in plancache's long-lived querytree
  `MemoryContext`, reached only via an opaque handle. The namespace owner's
  real impls are value-shaped (`GetSearchPathMatcher<'mcx>(Mcx<'mcx>) ->
  SearchPathMatcher<'mcx>`, `CopySearchPathMatcher<'mcx>(Mcx, &SearchPathMatcher)
  -> SearchPathMatcher<'mcx>`, `SearchPathMatchesCurrentEnvironment(Mcx,
  &mut SearchPathMatcher) -> bool`). The shapes are incompatible: the owner
  needs `Mcx`/owned `SearchPathMatcher<'mcx>`; plancache stores
  `SearchPathMatcherHandle` in `CachedPlanSource`, passes `CtxId`, and never
  has an `Mcx` at the call sites. Unifying onto the value shape requires
  redesigning the already merged/audited plancache's `CachedPlanSource` storage
  and all six call sites — a contract redesign of a downstream consumer.
  Tracked in `CONTRACT_RECONCILE_PENDING` (`backend_catalog_namespace`,
  {`get_search_path_matcher`, `copy_search_path_matcher`,
  `search_path_matches_current_environment`}).
- **Suggested fix**: in a joint plancache+namespace lane, replace the opaque
  `SearchPathMatcherHandle` with a value `SearchPathMatcher` carried in the
  querytree context (or thread the context as an `Mcx` to the namespace seam),
  then install the three seams from the owner and DELETE the three
  `CONTRACT_RECONCILE_PENDING` entries.
- **Branch**: fix/sw2-backend-catalog-namespace

## namespace `restrict_search_path` seam: blocked on unported GUC owner

- **Location**: `crates/backend-catalog-namespace-seams/src/lib.rs`
  (`restrict_search_path`), owned by `crates/backend-catalog-namespace`,
  consumed by `crates/backend-commands-cluster` and
  `crates/backend-commands-matview`.
- **Description**: `RestrictSearchPath()` is actually `utils/misc/guc.c`'s
  function (mis-homed onto the namespace seam crate). Its entire body is
  `set_config_option("search_path", GUC_SAFE_SEARCH_PATH, PGC_USERSET,
  PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false)`. It cannot be faithfully
  installed: the GUC owner (`backend-utils-misc-guc`) is unported, so the
  `set_config_option` seam is declared-but-uninstalled (panics at runtime);
  the existing `set_config_option(name, value, GucContext, GucSource)` seam
  lacks the `action` (GUC_ACTION_SAVE) / `changeVal` / `elevel` parameters this
  call requires; and the `GUC_SAFE_SEARCH_PATH` constant is unported. Tracked
  in `CONTRACT_RECONCILE_PENDING` (`backend_catalog_namespace`,
  `restrict_search_path`).
- **Suggested fix**: once `backend-utils-misc-guc` lands with a full
  `set_config_option` (action/changeVal/elevel) seam + `GUC_SAFE_SEARCH_PATH`,
  implement `RestrictSearchPath` in the namespace owner (guarded by
  `IsBootstrapProcessingMode`), install it, and DELETE the
  `CONTRACT_RECONCILE_PENDING` entry. (Or relocate the decl to a guc seam crate
  and home it on the guc owner.)
- **Branch**: fix/sw2-backend-catalog-namespace
## misc2 `make_expanded_object_read_only_internal` seam: bare-`Datum` vs byte-image divergence

- **Location**: `crates/backend-utils-adt-misc2-seams/src/lib.rs`
  (`make_expanded_object_read_only_internal`), owned by
  `crates/backend-utils-adt-misc2` (`expandeddatum::make_expanded_object_read_only_internal`),
  consumed by `crates/backend-executor-nodeValuesscan/src/lib.rs`
  (`MakeExpandedObjectReadOnly`).
- **Description**: The seam declares `(mcx, d: types_datum::Datum) ->
  PgResult<types_datum::Datum>`, mirroring C's `MakeExpandedObjectReadOnlyInternal(Datum)
  -> Datum` which dereferences the `Datum` pointer word to read/flip the varlena
  `va_tag`. But this repo's `types_datum::Datum` is `struct Datum(usize)` — a bare
  machine word with NO pointer/byte lane (a pass-by-reference varlena is "NOT
  representable here", per `types-datum/src/datum.rs`). The owner's real impl is
  therefore byte-image shaped: `(mcx, datum: &[u8]) ->
  PgResult<Option<PgVec<u8>>>`, consistent with the sibling expanded-object seams
  (`eoh_get_flat_size`/`eoh_flatten_into`) which cross as the
  `types_datum::ExpandedObjectRef<'_>` byte-slice handle built from real varlena
  bytes the caller holds. A thin adapter shim cannot bridge `Datum(usize)` <->
  varlena bytes without fabricating bytes from a `usize` (a forge/stub, violating
  opacity-inherited-never-introduced). The missing piece is the workspace-wide
  pointer-bytes datum-arena convention (same blocker as the composite-Datum
  bridge `DatumGetHeapTupleHeader`/`HeapTupleGetDatum` and the by-ref slot
  `slot_getattr` projection). So this one seam stays seam-and-panic; the
  byte-image impl is complete and tested. Tracked in
  `CONTRACT_RECONCILE_PENDING` (`backend_utils_adt_misc2`,
  `make_expanded_object_read_only_internal_v`).
- **Suggested fix**: once a pointer-bytes datum-arena lane lands on
  `types_datum::Datum` (the keystone that also unblocks the composite-Datum
  bridge and by-ref slot projection), either (a) change the seam decl + the
  nodeValuesscan `MakeExpandedObjectReadOnly` caller to cross as
  `ExpandedObjectRef`/byte image like the sibling seams, or (b) thread the
  pointer-bytes `Datum` through and add a real `Datum -> &[u8] -> Datum` adapter
  shim in the owner forwarding to the existing impl. Then install the seam and
  DELETE the `CONTRACT_RECONCILE_PENDING` entry so the guard re-asserts the install.
- **Branch**: fix/sw2-backend-utils-adt-misc2
## snapmgr-pre-seams `get_active_snapshot`: opaque-handle return diverges, uninstalled

- **Location**: `crates/backend-utils-time-snapmgr-pre-seams/src/lib.rs`
  (`get_active_snapshot`), nominal owner
  `crates/backend-utils-time-snapmgr`, consumed by
  `crates/backend-commands-prepare/src/lib.rs` (PREPARE/EXECUTE hands the
  result straight to `PortalStart`).
- **Description**: This is PREPARE/EXECUTE's own slice of snapmgr's
  `GetActiveSnapshot()`. Unlike the base `backend-utils-time-snapmgr-seams`
  copy (which crosses the real owned `Rc<RefCell<SnapshotData>>` and IS
  installed by `init_seams()`), the pre-seams copy returns the opaque
  `types_execparallel::SnapshotHandle` — a `handle!` newtype over a raw
  pointer-identity `usize`. The snapmgr owner models every live snapshot as an
  `Rc<RefCell<SnapshotData>>` with no pointer-identity registry, so it cannot
  mint that opaque handle without inventing a stand-in (forbidden:
  opacity-inherited-never-introduced). The seam therefore stays uninstalled and
  would panic on a real EXECUTE path. NOTE: the seam-install guard's
  install-detection matches the bare fn name `get_active_snapshot::set(`, finds
  the base-crate install in the same owner src, and so does NOT flag this — a
  name-collision blind spot. A `CONTRACT_RECONCILE_PENDING` entry would be
  reported STALE for the same reason, so this divergence is tracked here only.
- **Suggested fix**: When the portal/snapshot handle model is unified (cf.
  TD-PREPARE-1 and tcop-dest receiver-value keystone), resolve
  `types_execparallel::SnapshotHandle` to the real owned snapshot (or give
  snapmgr a pointer-identity registry that mints the handle), re-sign the
  pre-seams decl to the canonical type, and install it from `init_seams()`.
  Also tighten the guard's install-detection to disambiguate same-named seams
  declared in sibling split-tag crates.
- **Branch**: fix/sw2-backend-utils-time-snapmgr
## backend-utils-adt-datum-seams: seams-crate name does not match its owner dir (guard blind spot)

- **Description**: The bare-`Datum` lane of `datum.c` is declared in
  `backend-utils-adt-datum-seams` (`datum_copy` / `datum_estimate_space` /
  `datum_serialize` / `datum_restore` / `datum_image_hash` / `datum_image_eq`),
  but its runtime owner crate is `backend-utils-adt-scalar-datum-core`
  (CATALOG unit `probe-adt-scalar-datum`, status `audited`). All six seams ARE
  installed by that owner's `init_seams()` (wired into `init_all`), with
  signatures matching the decls verbatim, so no call path panics at runtime —
  including the `datum_estimate_space` "bare-word" variant, which is genuinely
  `::call`ed by `backend-access-nbtree-nbtree` and `backend-nodes-core`
  `copyParamList` (params.rs); it is NOT dead.
- **The debt**: `recurrence_guard::every_declared_seam_is_installed_by_its_owner`
  derives the owner dir by stripping the `-seams` suffix, so it attributes
  `backend-utils-adt-datum-seams` to a phantom `backend-utils-adt-datum` dir
  that does not exist and `continue`s at condition (a). The guard therefore
  provides ZERO coverage for these six installed seams: it can neither confirm
  the installs nor catch a future regression that drops one. (Related to the
  former `backend_timezone_pgtz :: pg_localtime` case — a seam declared in one
  crate but installed by its real C owner elsewhere — except the guard now
  recognizes a cross-crate `::set` as a valid install (workspace-wide
  `installed_seams`), so that allowlist line was deleted. Here the failure mode
  is different: the `-seams`-strip resolves to a PHANTOM dir, so the guard
  `continue`s at condition (a) and skips entirely, providing no coverage; an
  allowlist line would itself be flagged stale — none is added.)
- **Suggested fix**: rename `backend-utils-adt-datum-seams` to
  `backend-utils-adt-scalar-datum-core-seams` (or split out a
  `backend-utils-adt-scalar-datum-core-seams` crate holding these six decls and
  re-point the nbtree / nodes-core / brin-tuple consumers + the owner's
  `init_seams`), so the guard's `-seams`-strip resolves to the real owner dir
  and re-asserts the installs. Pure rename + dep-path churn; no logic change.
- **Branch**: fix/sw2-backend-utils-adt-scalar-datum-core
## backend-utils-adt-rangetypes: generic range I/O procs (range_in/out/recv/send) gated on the element-I/O fmgr lane

- **Seams**: `range_in`, `range_out`, `range_recv`, `range_send`
  (declared in `backend-utils-adt-rangetypes-seams`, owner
  `backend-utils-adt-rangetypes`). Consumed by
  `backend-utils-adt-multirangetypes::typcache_io` (multirange I/O routes each
  member range through these generic procs).
- **Blocker**: the real kernels in `crates/backend-utils-adt-rangetypes/src/range_io.rs`
  (`range_in`/`range_out`/`range_recv`/`range_send`) parse/render a range by
  invoking the *element subtype's* I/O proc through the fmgr Datum lane —
  `InputFunctionCallSafe`/`OutputFunctionCall`/`ReceiveFunctionCall`/`SendFunctionCall`
  on the cached `cache->typioproc` (rangetypes.c). That per-element fmgr dispatch
  is not ported into this unit, so the kernels deliberately mirror-pg-and-panic.
  Installing the seams would forward a call straight into a guaranteed panic, so
  they are held in `CONTRACT_RECONCILE_PENDING` instead of being force-wired.
- **Suggested fix**: once the element-I/O fmgr lane (calling a subtype's
  registered input/output/receive/send proc and threading its Datum result back)
  is available, fill the four `range_io.rs` kernel bodies, add the `::set(...)`
  installs to `init_seams()`, and DELETE the four
  `("backend_utils_adt_rangetypes", "range_*")` `CONTRACT_RECONCILE_PENDING`
  entries so the guard re-asserts the install.
- **Branch**: fix/sw2-backend-utils-adt-rangetypes
## plancache consumer seams: value/Mcx contract vs handle-registry owner (K1 #159)

- **Location**: all 16 consumer-facing seams in
  `crates/backend-utils-cache-plancache-seams/src/lib.rs`
  (`create_cached_plan`, `complete_cached_plan`, `save_cached_plan`,
  `drop_cached_plan`, `get_cached_plan`, `release_cached_plan`,
  `cached_plan_get_target_list`, `cached_plan_stmt_list`, and the field accessors
  `plansource_fixed_result` / `plansource_num_params` / `plansource_param_types` /
  `plansource_query_string` / `plansource_command_tag` / `plansource_result_desc` /
  `plansource_num_generic_plans` / `plansource_num_custom_plans`), owned by
  `crates/backend-utils-cache-plancache` (plancache.c).
- **Description**: The seam decls are VALUE-typed — they take `mcx: Mcx<'mcx>` and
  pass/return owned `'mcx` values (`RawStmt<'mcx>`, `Node<'mcx>`,
  `PlannedStmt<'mcx>`, `TupleDescData<'mcx>`, `mcx::PgVec<'mcx,_>`,
  `mcx::PgString<'mcx>`) keyed by an opaque `CachedPlanSourceHandle` /
  `CachedPlanHandle`. The merged/audited owner is built entirely on a handle
  REGISTRY: its real bodies (`CreateCachedPlan`, `CompleteCachedPlan`,
  `SaveCachedPlan`, `DropCachedPlan`, `GetCachedPlan`, `ReleaseCachedPlan`,
  `CachedPlanGetTargetList`) take/return handles (`RawStmtHandle`,
  `QueryListHandle`, `CtxId`, `TupleDescHandle`) into an internal
  `Rc<RefCell<CachedPlanSourceData>>` map and have NO `Mcx` parameter. The
  `plansource_*` and `cached_plan_stmt_list` field accessors have no owner function
  at all — the fields (`fixed_result`, `num_params`, `param_types`, `result_desc:
  TupleDescHandle`, `command_tag`, `num_generic_plans`, `num_custom_plans`) live on
  the registry-backed struct, not as standalone `'mcx`-allocated values. The owner
  therefore cannot `::set` any of these from its existing impls.
- **Why deferred**: Installing would require either (a) forging fake `'mcx` values
  out of the stored handles — a token/pointer-registry hack of exactly the kind
  opacity-inherited-never-introduced forbids — or (b) migrating plancache's whole
  `CachedPlanSource`/`CachedPlan` storage off opaque handles onto owned `'mcx`
  values (also retiring the `CtxId` context fields). That is the K1 plancache
  de-handle redesign, tracked as task #159. No thin value<->handle adapter exists.
  Tracked in `CONTRACT_RECONCILE_PENDING` as the 16
  `("backend_utils_cache_plancache", _)` entries.
- **Suggested fix**: under #159 (plancache de-handle + CtxId removal), choose the
  value `'mcx` boundary model end-to-end, migrate the owner's CachedPlanSource /
  CachedPlan storage and the pquery/utility consumers onto it, install all 16 seams
  in plancache's `init_seams()`, and DELETE the 16
  `("backend_utils_cache_plancache", _)` `CONTRACT_RECONCILE_PENDING` entries so the
  guard re-asserts the install.
- **Branch**: fix/sw2b-backend-utils-cache-plancache

## TD-GUC-UNPORTED: functioncmds GUC-array seams blocked on unported guc.c

- **Location**: `crates/backend-commands-functioncmds-seams/src/lib.rs`
  (`extract_set_variable_args`, `guc_array_add`, `guc_array_delete`,
  `guc_array_reset`), nominally owned by
  `crates/backend-commands-functioncmds`, consumed by
  `crates/backend-commands-functioncmds/src/ddl_core.rs` and
  `crates/backend-catalog-pg-db-role-setting/src/lib.rs`.
- **Description**: All four are `utils/misc/guc.c` functions
  (`ExtractSetVariableArgs`, `GUCArrayAdd`, `GUCArrayDelete`, `GUCArrayReset`)
  mis-homed onto the functioncmds seam crate because functioncmds was the first
  consumer. They have NO real implementation anywhere in the workspace: the GUC
  owner crate (`backend-utils-misc-guc`, guc.c / guc_tables) is unported
  (task #163). functioncmds's own `init_seams()` is empty (it owns no real impl
  of these), so the seams are declared-but-uninstalled and would panic at
  runtime on a real SET/RESET path. There is no faithful install without porting
  guc.c — stubbing the array-mutation / arg-flattening / superuser-reset logic
  would be inventing behaviour (forbidden). Tracked in
  `CONTRACT_RECONCILE_PENDING` (`backend_commands_functioncmds`,
  `{extract_set_variable_args, guc_array_add, guc_array_delete, guc_array_reset}`).
- **Suggested fix**: once `backend-utils-misc-guc` lands, re-home these four
  decls onto the guc owner's `-seams` crate, implement the real bodies there,
  wire its `init_seams()`, and DELETE the four
  `("backend_commands_functioncmds", ...)` `CONTRACT_RECONCILE_PENDING` entries
  so the guard re-asserts the install.
- **Branch**: fix/sw2b-backend-commands-functioncmds

## backend-storage-ipc-shm-toc: `shm_toc_estimate_{chunk,keys}` ParallelContext facade is un-installable

- **Seams**: `("backend_storage_ipc_shm_toc", "shm_toc_estimate_chunk")` and
  `("backend_storage_ipc_shm_toc", "shm_toc_estimate_keys")` in
  `CONTRACT_RECONCILE_PENDING`. Declared in `backend-storage-ipc-shm-toc-seams`
  keyed on `&mut types_nodes::ParallelContext`, `::call`ed by
  `backend-executor-nodeForeignscan` and `backend-executor-nodeCustom`
  (`Estimate{ForeignScan,CustomScan}` DSM-size paths).
- **Blocker**: the owned `types_nodes::ParallelContext` is the TRIMMED model —
  it carries only `toc: Opaque` (`Option<Box<dyn Any>>`, "storage-owned, opaque
  here") and has NO real `estimator: shm_toc_estimator` field. The shm-toc owner
  has genuine `shm_toc_estimate_chunk(e: &mut shm_toc_estimator, sz)` /
  `shm_toc_estimate_keys(e: &mut shm_toc_estimator, cnt)` impls, but there is no
  in-struct estimator reachable from `&mut ParallelContext` to operate on, so the
  owner cannot install this facade with real logic. The genuine estimate logic IS
  already wired: `backend-access-transam-parallel` keeps the real
  `shm_toc_estimator` in its own context store (addressed by
  `ShmTocEstimatorHandle`) and installs the handle-keyed
  `backend_access_transam_parallel_seams::shm_toc_estimate_{chunk,keys}` facade,
  delegating to `backend_storage_ipc_shm_toc::shm_toc_estimate_{chunk,keys}`.
  The `&mut ParallelContext` facade is a second, contract-divergent shape of the
  same C call. Force-installing it would require synthesizing an estimator inside
  the opaque box, diverging from the handle-store model.
- **Suggested fix**: with the ParallelContext de-handle keystone (give the owned
  `ParallelContext` a real `estimator: shm_toc_estimator` field, retiring the
  parallel crate's handle store), have the shm-toc owner install these two seams
  directly against `&mut pcxt.estimator` and DELETE the two
  `("backend_storage_ipc_shm_toc", "shm_toc_estimate_*")`
  `CONTRACT_RECONCILE_PENDING` entries so the guard re-asserts the install.
- **Branch**: fix/sw2b-backend-storage-ipc-shm-toc

## GetDatabasePath seam mis-homed onto catalog-catalog; provider unported (TD-GETDATABASEPATH)

- **Seam**: `get_database_path` (declared in `backend-catalog-catalog-seams`,
  attributed owner `backend-catalog-catalog`). Consumed in non-test code by
  `backend-utils-cache-inval::at_eoxact` (`AtEOXact_Inval` path) and
  `backend-utils-cache-relmapper` (`relmap_redo`).
- **Blocker**: `GetDatabasePath(dbOid, spcOid)` is `common/relpath.c`'s function,
  not `catalog/catalog.c`'s — the seam was mis-homed onto this owner's seam
  crate. Its genuine owner crate `backend-common-relpath` (relpath.c) is
  unported, and the canonical seam already has a value-shaped home in
  `backend-common-relpath-seams` (`get_database_path<'mcx>(Mcx, Oid, Oid) ->
  PgResult<PgString<'mcx>>`). The catalog-catalog copy's frozen contract is
  `(db_oid, spc_oid) -> PgResult<String>` (owned String, no Mcx). Installing the
  path arithmetic in catalog-catalog would re-home relpath.c's logic into the
  wrong translation unit, and the two seam contracts diverge (owned String vs
  Mcx/PgString) so no thin adapter unifies them.
- **Suggested fix**: port `common/relpath.c` as `backend-common-relpath`, install
  `backend_common_relpath_seams::get_database_path` there, migrate the inval/
  relmapper consumers off `backend_catalog_catalog_seams::get_database_path` onto
  the relpath seam, then DELETE the catalog-catalog seam decl and the
  `("backend_catalog_catalog", "get_database_path")`
  `CONTRACT_RECONCILE_PENDING` entry so the guard re-asserts.
- **Branch**: fix/sw2b-backend-catalog-catalog

## backend-replication-walreceiverfuncs: xlog_request_wal_receiver_reply owned by the unported xlogrecovery TU

- **Seam**: `xlog_request_wal_receiver_reply`
  (declared in `backend-replication-walreceiverfuncs-seams`, attributed to owner
  `backend-replication-walreceiverfuncs`). Consumed by
  `backend-access-transam-xact::redo` (remote_apply feedback during WAL redo).
- **Blocker**: the seam's real body is `XLogRequestWalReceiverReply()` in
  `access/transam/xlogrecovery.c`, NOT `walreceiverfuncs.c`. The walreceiverfuncs
  owner explicitly documents this and deliberately does not `::set` it (see the
  `init_seams` NOTE in `crates/backend-replication-walreceiverfuncs/src/lib.rs`).
  The true owner crate `backend-access-transam-xlogrecovery` is unported — only
  its empty `-seams` crate exists — so there is no impl to install. Force-wiring
  from walreceiverfuncs would mean inventing a stand-in body in the wrong TU
  (forbidden), so the seam is held in `CONTRACT_RECONCILE_PENDING` instead.
- **Suggested fix**: when `backend-access-transam-xlogrecovery` lands, port
  `XLogRequestWalReceiverReply()` there, install it from xlogrecovery's
  `init_seams()`, and DELETE the
  `("backend_replication_walreceiverfuncs", "xlog_request_wal_receiver_reply")`
  `CONTRACT_RECONCILE_PENDING` entry so the guard re-asserts the install.
- **Branch**: fix/sw2b-backend-replication-walreceiverfuncs

## TD-EXECEXPR-PARAMSETEQ: exec_build_param_set_equal seam decl pre-dates the owned-builder model

- **Seam**: `exec_build_param_set_equal`
  (declared in `backend-executor-execExpr-seams`, attributed to owner
  `backend-executor-execExpr`). Consumed by `backend-executor-nodeMemoize`
  (`build_cache_eq_expr`, the non-binary Memoize cache key-equality program).
- **Blocker**: the seam decl still carries the pre-owned-model `ExecBuildParam-
  SetEqual` C shape — trailing `parent: &mut PlanStateData` + `estate: &mut
  EStateData` and NO leading `mcx` — and nodeMemoize calls it with exactly that
  shape. The owner's real body (`execExpr_domain_agg::exec_build_param_set_equal`)
  was rewritten onto this crate's owned-builder convention (`mcx`-first, the
  result `desc`/key `ops` passed directly, no `parent`/`estate` — the C `parent`
  is only used for slot descriptors / SubPlan attribution). This is the same
  reconciliation the installed sibling `exec_build_hash32_expr` already received.
  No thin adapter can bridge the two: the call site supplies `parent`/`estate`
  the impl does not take and lacks the `mcx` the impl needs.
- **Suggested fix**: reconcile the decl + nodeMemoize call site onto the owned
  shape (`mcx`-first, drop `parent`/`estate`), then install
  `seams::exec_build_param_set_equal::set(execExpr_domain_agg::exec_build_param_set_equal)`
  in execExpr's `init_seams()` and DELETE the
  `("backend_executor_execExpr", "exec_build_param_set_equal")`
  `CONTRACT_RECONCILE_PENDING` entry. This is part of the executor de-handle /
  contract-reconcile work (#112, #167/#169), not pure seam wiring.
- **Branch**: fix/seam-wiring-v2 (dim-4 hardened-guard sweep)

## TD-XLOGRECOVERY-PAGEREAD: recovery ReadRecord seams blocked on the unported WAL page-read driver

- **Seams** (declared, `::call`ed by the recovery driver's `ReadRecord` loop,
  NOT installed):
  - `backend_access_transam_xlogprefetcher::prefetcher_begin_read`
  - `backend_access_transam_xlogprefetcher::prefetcher_read_record`
  - `backend_access_transam_xlogreader::xlog_rec_rmid`
  - `backend_access_transam_xlogreader::xlog_rec_info`
  - `backend_access_transam_xlogreader::xlog_rec_total_len`
- **Consumer**: `backend-access-transam-xlogrecovery` (xlogrecovery #13 F1,
  `readrecord.rs`): `read_record` calls `prefetcher_read_record`, then
  `read_checkpoint_record` calls `prefetcher_begin_read` + the three
  `xlog_rec_*` accessors.
- **Blocker**: the two prefetcher seams are declared in
  `backend-access-transam-xlogprefetcher-seams` with the explicit note "NOT
  installed: the page-read driver is not yet ported." The CATALOG-`merged`
  `xlogprefetcher` unit ported only the prefetch-stats shmem
  (`XLogPrefetchShmemSize`/`Init`); the recovery read-record entry points wrap
  the genuinely-unported hard-core WAL file I/O — `XLogPageRead` /
  `WaitForWALToBecomeAvailable` / `XLogFileRead{,AnyTLI}` plus
  `restore_command` archive fetching. The three `xlog_rec_*` accessors are
  keyed by the opaque `RecordRef(u64)` handle into the externally-owned
  decoded record, but the merged `xlogreader` models the record as a borrowed
  `&XLogReaderState`, not a handle registry; the `RecordRef`->reader mapping is
  owned by that same unported page-read driver. Installing either set without
  the driver requires a forbidden token registry / a contract redesign.
- **Suggested fix**: when the page-read driver (the xlogprefetcher recovery leg
  + its `RecordRef` handle registry over the reader) lands, install all five
  seams in their owners' `init_seams()` and DELETE the five
  `CONTRACT_RECONCILE_PENDING` entries. This is the sanctioned hard-core WAL
  I/O deferral (xlog #111 F3 policy), not pure seam wiring.
- **Branch**: xlogrec-f1 (xlogrecovery #13 F1 readrecord)

### UPDATE (campaign C4, wf-walread): the page-read DRIVER is now ported

The hard-core WAL file-read driver itself is ported into
`backend-access-transam-xlogrecovery::pageread` (new module): `XLogPageRead`
(the reader `page_read` callback), `WaitForWALToBecomeAvailable` (the standby
source state machine), `XLogFileRead` / `XLogFileReadAnyTLI`, and
`rescanLatestTimeLine`, all 1:1 from xlogrecovery.c (3320-4052, 4148-4233,
4235-4410). The C file-static read cursor (`readFile`/`readSegNo`/`readOff`/
`readLen`/`readSource`/`lastSourceFailed`/`flushedUpto`/`receiveTLI`/
`curFileTLI`/`InRedo` + the `last_fail_time` function-static) lives as
thread-locals; `XLogPageReadPrivate` is the reader `private_data`; the
mode/option globals are reached through a single-startup-process raw
`*mut XLogRecoveryState` thread-local (mirrors the crate's existing
`shmem.rs::ctl_ptr` idiom). The archive/pg_wal (crash-recovery) leg is fully
faithful; the streaming legs seam-and-panic into the unported walreceiverfuncs.c
(`request_xlog_streaming`/`wal_rcv_streaming`/`xlog_shutdown_wal_rcv`/
`set+reset_install_xlog_file_segment_active`/`get_wal_rcv_flush_rec_ptr_full`),
the live-prefetcher `prefetcher_compute_stats`, and `RequestCheckpoint`
(checkpointer `todo`). New seams installed by their real owners now:
`backend-access-transam-xlogarchive::restore_archived_file`,
`backend-storage-file-fd::close_fd`,
`backend-storage-ipc-procarray::known_assigned_transaction_ids_idle_maintenance`.

**Residual blocker (the reader/prefetcher holder keystone)** — the FIVE seams
above stay in `CONTRACT_RECONCILE_PENDING`: installing them (and retiring the
`RecordRef` registry) requires the recovery process to OWN a live
`XLogReaderState` + `XLogPrefetcher` instance, allocated by `InitWalRecovery`
(unported) with `routine.page_read = pageread::x_log_page_read`. The prefetcher
holds `&'r mut XLogReaderState` (a self-borrow if stored in one thread-local),
so the holder needs the audited raw-pointer escape over a process-lifetime
reader (matching C's permanent-context palloc). Plus `xlogreader` lacks
`XLogRecGetRmid/Info/TotalLen` accessors over `reader.record` (only the decode
queue is modelled). Once that holder lands, `readrecord.rs` drives the owned
prefetcher directly (no seam), the five seams install over the held instance,
and the `RecordRef(u64)` carrier becomes a thin handle resolved against the real
`&XLogReaderState`. New `CONTRACT_RECONCILE_PENDING` additions
(`prefetcher_compute_stats` + the 6 walreceiverfuncs streaming seams) clear when
walreceiverfuncs.c lands.

### RESOLVED (campaign C4 tail, wf-walreaderholder): holder + accessors landed

The reader/prefetcher holder is built in
`backend-access-transam-xlogrecovery::walrecovery` (new module): the
`InitWalRecovery` reader/prefetcher allocation leg
(`init_wal_recovery_reader`) allocates the recovery process's live
`XLogReaderState` (`routine.page_read = pageread::x_log_page_read`,
`private_data = XLogPageReadPrivate`) in a process-lifetime leaked
`MemoryContext`, then builds the `XLogPrefetcher` over it. The prefetcher's
`&'r mut XLogReaderState` self-borrow is resolved with the audited raw-pointer
escape (two `*mut` thread-locals, the `shmem.rs::ctl_ptr` idiom) over the
single-threaded startup process. `xlogreader` gained `XLogRecGetRmid` /
`XLogRecGetInfo` / `XLogRecGetTotalLen` accessors over `reader.record`.

All SIX seams are now INSTALLED from the holder (a sanctioned cross-crate
install — only the holder can resolve a `RecordRef` against the live reader):
`prefetcher_begin_read`, `prefetcher_read_record`, `prefetcher_compute_stats`,
`xlog_rec_rmid`, `xlog_rec_info`, `xlog_rec_total_len`. Their
`CONTRACT_RECONCILE_PENDING` entries are DELETED. The `RecordRef(u64)` registry
is RETIRED: the type is no longer a side-map handle — `RecordRef(0)` is the C
NULL (no current record) and any non-zero value names "the held reader's
current decoded record", resolved directly against `reader.record` (the
prefetcher exposes exactly one current record at a time, as the C
`XLogRecGetXXX(xlogreader)` macros read `xlogreader->record`). No side registry
is kept. The remaining `walreceiverfuncs` streaming seams still seam-and-panic
until walreceiverfuncs.c lands (reachable only once recovery actually streams).

## TD-PARSETYPE-RAWGRAMMAR: parse_type.c raw_parser type-name drive — RESOLVED

- **Seam**: `backend_parser_driver::raw_parse_type_name` (declared in
  `backend-parser-driver-seams`).
- **Resolution**: `gram.y` landed (`base_yyparse` real + installed, handles
  `MODE_TYPE_NAME`), so `backend-parser-driver` now installs
  `raw_parse_type_name` from its `init_seams()`. The install drives
  `raw_parser(str, RAW_PARSE_TYPE_NAME)` in a private `MemoryContext`, pulls the
  single `TypeName` node out of the `RawStmt` wrapper, and bridges the arena
  `types_nodes::rawnodes::TypeName<'mcx>` into the owned
  `types_parsenodes::TypeName` (the arena->owned reconcile mirrors
  parse_type.c's `raw_typename_to_parse`) before the context drops. The
  `CONTRACT_RECONCILE_PENDING` allowlist entry was deleted.

## table_index_build_scan provider unported (TD-INDEXBUILDSCAN)

- **What**: `backend-access-table-tableam :: table_index_build_scan` is declared
  + `::call`ed (by `hashbuild` / `hashbuildempty` in
  `backend-access-hash-entry`, mirroring nbtree) but NOT installed.
- **Why**: `table_index_build_scan` (tableam.h) dispatches the heap AM's
  `heapam_index_build_range_scan` callback (`heapam_handler.c`), which is still
  `todo` (the heap table-AM vtable is not ported). Installing it requires the
  heap AM handler to land.
- **Effect**: building a hash index reaches the still-unported heap
  index-build scan and panics (mirror-pg-and-panic). All hash AM internal logic
  (insert/search/page/ovfl/bucket-split) is fully ported and unaffected.
- **Suggested fix**: install `table_index_build_scan` in the tableam owner's
  `init_seams()` once `heapam_handler.c` lands, and DELETE the
  `CONTRACT_RECONCILE_PENDING` entry.
- **Branch**: port/backend-access-hash-core + port/backend-access-hash-entry

## K1 DONE: table-AM analyze-scan vtable slots + heap provider landed

- **What**: the ANALYZE-sampling table-AM primitives are now ported. Added the
  `scan_analyze_next_block` / `scan_analyze_next_tuple` slots to
  `types_tableam::TableAmRoutine` (mcx-vtable convention, #289), ported the heap
  provider bodies `heapam_scan_analyze_next_block` / `heapam_scan_analyze_next_tuple`
  (`backend-access-heap-heapam-handler-core::analyze_scan`, faithful
  branch-for-branch HEAPTUPLE_LIVE/DEAD/RECENTLY_DEAD/INSERT_IN_PROGRESS/
  DELETE_IN_PROGRESS classification over bufmgr + the heap scan state), and added
  the `table_beginscan_analyze` / `table_scan_analyze_next_block` /
  `table_scan_analyze_next_tuple` dispatch + seams (`backend-access-table-tableam`
  installs all three from `init_seams`) for `commands/analyze.c`'s
  `acquire_sample_rows` to consume.
- **ReadStream layering note**: PG18.3's `scan_analyze_next_block` C signature
  takes `ReadStream *`, whose only use in the heap callback is
  `read_stream_next_buffer(stream, NULL)`. `ReadStream`
  (`backend-storage-aio-read-stream`) lives far above the `types-tableam` /
  `tableam-seams` layer, so the stream crosses both the vtable slot and the seam
  as the `next_buffer: &mut dyn FnMut() -> PgResult<Buffer>` closure the
  `analyze.c` owner builds over its stream — the same closure-across-layers
  technique the index-build callback uses (`opacity-inherited-never-introduced`:
  no invented handle; the higher-layer type is simply not nameable here).
- **K2 value lane UNIFIED (analyze.c now re-fireable)**: the AM scan primitives
  `acquire_sample_rows` drives are in place, **K2** (`VacAttrStats` /
  `StatsBuildData`, commit `55df0f808`) has landed, and the statistics value lane
  is now unified onto the canonical `'mcx` byte-lane enum
  `types_tuple::backend_access_common_heaptuple::Datum<'mcx>` (`ByVal(usize)` /
  `ByRef(PgVec<u8>)`). The earlier contract-divergent bare-word
  `types_datum::Datum(usize)` carriers — `VacAttrStats.stavalues` / `.exprvals`,
  `StatsBuildData.values`, `MCVItem`/`SortItem.values`, and `AnalyzeAttrFetchFunc`
  — were re-typed to `Datum<'mcx>` (`MCVItem`/`MCVList`/`SortItem` gained the
  `'mcx` lifetime). By-ref text/numeric/varchar stats values now round-trip
  safely through the byte lane (the dangling-pointer corruption is gone). The
  three `backend-statistics-core` seams that touch values
  (`statext_mcv_build` → `MCVList<'mcx>`, `mcv_compare_scalars_simple` /
  `mcv_value_to_serialized_bytes` → `&Datum<'mcx>`, `mcv_serialized_bytes_to_value`
  → `Datum<'mcx>`, `mcv_get_match_bitmap` → `MCVList<'mcx>`) were re-signed; the
  two fmgr-return seams (`pg_stats_ext_mcvlist_items` / `pg_mcv_list_out`) keep
  bare-word `types_datum::Datum` (the irreducible `PGFunction`-return ABI edge).
  All three merged consumer crates (`backend-statistics-{mcv,mvdistinct,
  dependencies}`) compile and their unit tests (14 / 15 / 16) pass unmodified —
  the serialize byte layout (MAGIC / format constants) is unchanged. ANALYZE's
  value flow (`datum_copy` / `apply_sort_comparator` / `DeformedColumn`) is now
  type-compatible with the carrier; `backend-commands-analyze` is **re-fireable**.
  See memory `analyze-c-blocked-on-bareword-datum-carrier.md` (the prerequisite
  keystone it described is now satisfied).
- **No CONTRACT_RECONCILE_PENDING change**: re-typing carrier fields + re-signing
  the (uninstalled, owner-pending) statistics seams adds/retires nothing in the
  reconcile ledger.

## TD-PATHNODE-JOINRELS-GAP: pathnode.c can_create_unique_path / install_dummy_append_path unported

- **Location**: `crates/backend-optimizer-util-pathnode-seams/src/lib.rs`
  (`can_create_unique_path`, `install_dummy_append_path`) consumed by
  `crates/backend-optimizer-path-joinrels/src/lib.rs` (`join_is_legal`,
  `mark_dummy_rel`, `populate_joinrel_with_paths`); nominal owner
  `crates/backend-optimizer-util-pathnode` (`backend_optimizer_util_pathnode`).
- **Description**: the join-relation enumerator (joinrels.c) needs two
  pathnode.c routines that the COMPLETE pathnode crate has not yet ported:
  `can_create_unique_path` (the cached `create_unique_path(..., NULL)` probe
  used by the semijoin unique-ify legality checks) and
  `install_dummy_append_path` (the `create_append_path` childless-Append +
  `add_path`/`set_cheapest` recost performed in the rel's own memory context
  by `mark_dummy_rel`). Their bodies pull in create_unique_path /
  create_append_path / the GetMemoryChunkContext switch, none of which the
  pathnode port has reached. joinrels seam-and-panics through
  `backend-optimizer-util-pathnode-seams` until the owner lands them. Tracked in
  `CONTRACT_RECONCILE_PENDING` (2 entries) rather than force-wired.
- **Suggested fix**: port `can_create_unique_path` and
  `install_dummy_append_path` into `backend-optimizer-util-pathnode`, install
  both in its `init_seams()`, and DELETE the two
  `CONTRACT_RECONCILE_PENDING` entries.
- **Branch**: port/backend-optimizer-path-joinrels

## TD-GIN-EXTRACT-QUERY: GIN extractQueryFn fmgr dispatch is uninstalled
- **Seam**: `gin_extract_query` declared in
  `crates/backend-access-gin-ginutil-seams` (the GIN substrate seam crate),
  consumed by `crates/backend-access-gin-ginscan/src/lib.rs` (`ginNewScanKey`).
- **Description**: GIN's `ginNewScanKey` runs the opclass `extractQueryFn`
  through `FunctionCall7Coll(...)` with five by-pointer out-params
  (`&nentries`, `&partial_matches`, `&extra_data`, `&nullFlags`,
  `&searchMode`). That fmgr GIN-call dispatch is genuinely external — the SAME
  unported owner (the fmgr GIN-call dispatcher) as the already-uninstalled
  `gin_extract_value` / `gin_compare_entries` / `gin_consistent_call_{bool,tri}`
  GIN substrate seams. The seam is declared in `ginutil-seams` because that is
  the GIN substrate seam crate; the recurrence guard attributes it to the
  COMPLETE `ginutil` owner, but `ginutil` does not call it (`ginscan` does), so
  the OUTWARD-seam exclusion that silently covers the sibling gin substrate
  seams does not fire. It loud-panics (mirror-pg-and-panic) until the fmgr GIN
  dispatcher lands. Tracked in `CONTRACT_RECONCILE_PENDING` (1 entry).
- **Suggested fix**: install `gin_extract_query` (and the sibling
  `gin_extract_value` / `gin_compare_entries` / `gin_consistent_call_*`) from
  the fmgr GIN-call dispatcher when it lands, and DELETE this
  `CONTRACT_RECONCILE_PENDING` entry.
- **Branch**: wf-gin (GIN L1c-ginscan)

## TD-GIN-COMPARE-PARTIAL: GIN comparePartialFn fmgr dispatch is uninstalled
- **Seam**: `gin_compare_partial` declared in
  `crates/backend-access-gin-ginutil-seams` (the GIN substrate seam crate),
  consumed by `crates/backend-access-gin-ginget/src/lib.rs:454,1465`
  (`collectMatchBitmap` / `matchPartialInPendingList`).
- **Description**: GIN's partial-match scan runs the opclass `comparePartialFn`
  through the inline
  `DatumGetInt32(FunctionCall4Coll(&ginstate->comparePartialFn[attnum-1],
  collation, queryKey, idatum, UInt16GetDatum(strategy),
  PointerGetDatum(extra_data)))` (ginget.c:193, ginget.c:1592). That fmgr
  GIN-call dispatch is genuinely external — the SAME unported owner (the fmgr
  GIN-call dispatcher) as the already-uninstalled `gin_extract_value` /
  `gin_compare_entries` / `gin_extract_query` GIN substrate seams. The seam is
  declared in `ginutil-seams` because `ginutil` owns `comparePartialFn` via
  `initGinState` (ginutil.c:198); the recurrence guard attributes it to the
  COMPLETE `ginutil` owner, but `ginutil` does not call it (`ginget` does), so
  the OUTWARD-seam exclusion that silently covers the sibling gin substrate
  seams (which `ginutil` DOES call) does not fire. It loud-panics
  (mirror-pg-and-panic) until the fmgr GIN dispatcher lands. Tracked in
  `CONTRACT_RECONCILE_PENDING` (1 entry).
- **Suggested fix**: install `gin_compare_partial` (and the sibling
  `gin_extract_value` / `gin_compare_entries` / `gin_extract_query` /
  `gin_consistent_call_*`) from the fmgr GIN-call dispatcher when it lands, and
  DELETE this `CONTRACT_RECONCILE_PENDING` entry.
- **Branch**: fix-red-main (seams-init recurrence guard repair)

## TD-HEAPAM-SUBFAMILY-SEAMS: 6 heapam seams have no owner body (sub-families unported)
- **Seams**: `heap_multi_insert`, `index_compute_xid_horizon_for_tuples`,
  `insert_one_tuple`, `log_heap_visible`, `read_pg_type`, `scan_indisclustered`,
  declared in `crates/backend-access-heap-heapam-seams`, attributed by the
  recurrence guard to the COMPLETE `backend-access-heap-heapam` owner.
- **Description**: The owner crate holds none of these bodies; their C logic
  lives in heapam sub-families NOT ported into this owner slice:
  `index_compute_xid_horizon_for_tuples` (heapam.c index LP_DEAD xid-horizon
  family; consumed by gist_insert / hashinsert); `log_heap_visible` (heapam.c
  VM-WAL family, `XLOG_HEAP2_VISIBLE`; consumed by visibilitymap);
  `insert_one_tuple` (bootstrap.c `InsertOneTuple` form+`simple_heap_insert`
  batch — the owner's NB at `lib.rs:509` documents it as intentionally
  uninstalled; consumed by backend-bootstrap-bootstrap); `read_pg_type`
  (bootstrap.c `populate_typ_list` catalog scan; consumed by
  backend-bootstrap-bootstrap); `scan_indisclustered` (cluster.c
  `get_tables_to_cluster` pg_index scan; consumed by backend-commands-cluster);
  `heap_multi_insert` (heapam.c's slot-based batch heap insert — one WAL record
  per page, buffer extension, toast via `heap_prepare_insert`, VM clears; the
  owner slice ports only the page-count helper `heap_multi_insert_pages`, not
  the batch engine; consumed by `CatalogTuplesMultiInsertWithInfo` in
  backend-catalog-indexing, which is itself the engine behind the three
  `catalog_tuples_multi_insert_pg_{depend,shdepend,enum}` family seams).
  None is `::set` anywhere and none has a non-panicking owner body, so each
  loud-panics (mirror-pg-and-panic) on a real call path. Tracked in
  `CONTRACT_RECONCILE_PENDING` (6 entries).
- **Suggested fix**: install each from its real owner when bootstrap.c /
  cluster.c / the heapam VM-WAL + index-xid-horizon legs are ported, and DELETE
  the matching `CONTRACT_RECONCILE_PENDING` entries.
- **Branch**: fix-red-main (seams-init recurrence guard repair)

## TD-ANALYZE-PLANCACHE-HANDLE-SEAMS: 19 analyze seams uninstallable (handle model + rewriter)

**MOSTLY RESOLVED (#159 STEP C plancache de-handle):** the 16 plancache-facing handle
forms + the field-projection seams (`query_*`, `stmt_requires_parse_analysis`,
`analyze_requires_snapshot`, `query_requires_rewrite_plan`, `walk_query_sublinks_for_locks`,
`analyze_and_rewrite_fixedparams`, `analyze_and_rewrite_withcb`) are no longer called:
plancache owns `RawStmt<'static>`/`Query<'static>` values, calls the value seams
`stmt_requires_parse_analysis_value` / `analyze_requires_snapshot_value` /
`query_requires_rewrite_plan_value` / `pg_analyze_and_rewrite_fixedparams_params`, reads
Query fields directly, and walks sublinks via `node_walker`. Their allowlist entries were
deleted. STILL DEFERRED: `analyze_and_rewrite_varparams` (unported varparam rewriter leg)
and `run_post_parse_analyze_hook` (NULL-by-default hook with no body) remain allowlisted.
- **Seams**: 14 `backend-parser-analyze-pc-seams` field/predicate projections
  (`query_can_set_tag`, `query_command_type_is_utility`, `query_cte_queries`,
  `query_has_cte_list`, `query_has_rtable`, `query_has_sublinks`,
  `query_requires_rewrite_plan`, `query_returning_list`, `query_rtable_fields`,
  `query_target_list`, `query_utility_stmt`, `walk_query_sublinks_for_locks`,
  `stmt_requires_parse_analysis`, `analyze_requires_snapshot`) plus 3
  `backend-parser-analyze-seams` legs (`pg_analyze_and_rewrite_fixedparams`,
  `analyze_and_rewrite_varparams`, `analyze_and_rewrite_withcb`,
  `run_post_parse_analyze_hook` — note `analyze_and_rewrite_withcb` is also
  pc-seams). The recurrence guard resolves the `-pc-seams` dir to the COMPLETE
  `backend-parser-analyze` owner.
- **Description**: The 14 pc-seams are typed on `types_plancache::*Handle`
  (`RawStmtHandle` / `QueryHandle` / `AnalyzedQueryHandle` / ...); the analyze
  owner does NOT depend on `backend-parser-analyze-pc-seams` and has no handle
  producer, and the only field-projection logic it does hold
  (`stmt_requires_parse_analysis(&RawStmt)` / `analyze_requires_snapshot(&RawStmt)`
  / `query_requires_rewrite_plan(&Query)`) is value-typed, incompatible with the
  handle signatures. These are the plancache #159 de-handle keystone's seams —
  their sole consumer is the unported `backend-utils-cache-plancache` (see the
  `plancache-159-blocked-on-parser-planner-value-producers` memory note). The 3
  owner `-seams` legs (`pg_analyze_and_rewrite_fixedparams` /
  `analyze_and_rewrite_varparams` / `run_post_parse_analyze_hook`) have NO body:
  their C legs call `pg_rewrite_query` (rewriter unported) and the deferred
  jumble + post-parse-analyze-hook subsystem. All loud-panic
  (mirror-pg-and-panic). Tracked in `CONTRACT_RECONCILE_PENDING` (19 entries).
- **Suggested fix**: install the 14 field/predicate seams from plancache once
  the #159 de-handle keystone retypes them onto owned `Query<'mcx>`/`RawStmt<'mcx>`
  values (then the owner's value-typed bodies satisfy them); install the rewrite
  legs once `pg_rewrite_query` + the jumble/post-parse hook land. DELETE the
  matching `CONTRACT_RECONCILE_PENDING` entries as each lands.
- **Branch**: fix-red-main (seams-init recurrence guard repair)

## TD-BUFMGR-SHMEM-GUC-SEAMS: 4 bufmgr seams uninstallable (NBuffers global + GUC/aio unported)
- **Seams**: `buffer_manager_shmem_init`, `buffer_manager_shmem_size`,
  `io_method_sync`, declared in
  `crates/backend-storage-buffer-bufmgr-seams`, attributed to the COMPLETE
  `backend-storage-buffer-bufmgr` owner.
- **Description**: `buffer_manager_shmem_init` (C `BufferManagerShmemInit(void)`)
  and `buffer_manager_shmem_size` (C `BufferManagerShmemSize(void)`) both read
  the file-global `NBuffers`, which has no global home in this repo — every
  sub-sizer takes `nbuffers` explicitly. The owner exposes only a
  `BufferManagerShmemInit(nbuffers) -> Self` constructor + `register_global`
  ambient publish, plus partial sub-sizers (`StrategyShmemSize` /
  `BufTableShmemSize` in buffer-support); there is no full `BufferManagerShmemSize`
  body, and the shmem-bootstrap caller (ipci `CreateOrAttachShmemStructs`) is
  unported. (The `buffer_manager_shmem_size` seam sig `() -> PgResult<Size>`
  actually matches C `Size BufferManagerShmemSize(void)` — the blocker is the
  missing body + absent global NBuffers, not a sig divergence.) `io_method_sync`
  (`io_method == IOMETHOD_SYNC`) and `maintenance_io_concurrency` are outward
  GUC/aio seams whose live-value owners (aio.c io_method / the GUC value-read
  path) are unported — installed only in buffer-support `test_support` today,
  exactly like the sibling `effective_io_concurrency` / `io_combine_limit` /
  `io_direct_data` GUC seams. All loud-panic (mirror-pg-and-panic). Tracked in
  `CONTRACT_RECONCILE_PENDING` (4 entries).
- **Suggested fix**: establish a GUC-backed global `NBuffers`, write a full
  `BufferManagerShmemSize`, and install both shmem seams from the ipci shmem
  bootstrap when it lands; install `io_method_sync` from the aio io_method owner
  (aio.c) and `maintenance_io_concurrency` from the GUC value-read path when they
  land. DELETE the matching `CONTRACT_RECONCILE_PENDING` entries.
- **Branch**: fix-red-main (seams-init recurrence guard repair)

## TD-GIN-OPTIONS-RELCACHE: GIN reloption getters uninstalled (GinOptions relcache keystone)
- **Seams**: `gin_get_use_fast_update`, `gin_get_pending_list_cleanup_size`,
  declared in `crates/backend-access-gin-ginutil-seams` (ginutil owns
  `GinOptions`), `::call`ed from `crates/backend-access-gin-ginfast`
  (`ginHeapTupleFastInsert` / `ginInsertCleanup` reloption reads, mirroring C
  `GinGetUseFastUpdate` / `GinGetPendingListCleanupSize`).
- **Description**: both read the GIN `GinOptions` bytea off the index relcache
  entry (`rd_options`); the GinOptions relcache keystone (parsing `rd_options`
  into a `GinOptions` struct) is unported, so the COMPLETE/audited
  `backend-access-gin-ginutil` owner has no body and never `::set`s them. The
  ginfast fast-update path routes its fastupdate/cleanup-size decision through
  these seams, which loud-panic (mirror-pg-and-panic) on a real call. The
  recurrence guard attributes them to the COMPLETE ginutil owner, but ginutil
  does not call them (ginfast does), so the OUTWARD-seam exclusion does not fire.
  Tracked in `CONTRACT_RECONCILE_PENDING` (2 entries). (`backend-access-gin-ginfast`
  also declares a `gin_get_use_fast_update` in `gininsert-seams` that it DOES
  install — that one routes here; the keystone-blocked pair is the `ginutil-seams`
  decls.)
- **Suggested fix**: install both reloption getters from ginutil once the
  GinOptions relcache parse path lands, and DELETE these
  `CONTRACT_RECONCILE_PENDING` entries.
- **Branch**: fix-red-main (seams-init recurrence guard repair)

## TD-DEPENDENCY-REMOVEFUNC + TD-SYSCACHE-DYNAMIC-TID (backend-catalog-dependency)

dependency.c's `doDeletion` (OCLASS_PROC) and `DropObjectById` call two seams
whose real owners are unported:

- **`backend_commands_functioncmds::remove_function_tuple`** — the pg_proc
  `RemoveFunctionById` catalog delete. functioncmds.c is only a *consumer* of
  this seam (it calls it at ddl_core.rs:1163); the real owner is pg_proc.c's
  `RemoveFunctionById`, which is not ported, so nobody installs it.
- **`backend_utils_cache_syscache::search_syscache1_tid`** — a new generic
  primitive `SearchSysCache1(cacheId, ObjectIdGetDatum(key))` for a *dynamic*
  cacheId, returning the matched tuple's `t_self` for `CatalogTupleDelete`.
  The syscache owner models its caches statically; this dynamic-cacheId
  primitive is not ported, so it is not installed.

- **Status**: both `CONTRACT_RECONCILE_PENDING` allowlist entries; loud-panic
  on a real call path until their owners land.
- **Suggested fix**: install `remove_function_tuple` when pg_proc's
  `RemoveFunctionById` lands; install `search_syscache1_tid` when the
  dynamic-cacheId `SearchSysCache1` primitive lands. Then DELETE these entries.
- **Branch**: wf-catdep (backend-catalog-dependency port)

## backend-catalog-dependency: findDependentObjects scan recheck model

dependency.c's `findDependentObjects` keeps a live `systable` scan open while it
releases a deletion lock, re-locks the owner/dependent, and calls
`systable_recheck_tuple(scan, tup)` to confirm the row survived. This port
materialises each pg_depend scan's rows up front (`scan_depend_rows`), because
the C control flow closes the scan before recursing — so the live-scan recheck
seam (`genam::systable_recheck_tuple`, which takes `&mut SysScanDescData`)
cannot be threaded through. `recheck_pg_depend` re-fetches the exact row by its
full key instead, which is the same visibility-after-lock guarantee the C
recheck provides. Reconcile to the live-scan recheck if/when the scan is kept
open across the lock release.

Also: `deleteOneObject`'s `PERFORM_DELETION_CONCURRENTLY` close/reopen of the
shared `depRel` is modelled by opening pg_depend fresh for the
outgoing-link-delete scan (the relation is never held across `doDeletion`), so
the concurrent close/reopen is implicit in the owned-`Relation` guard scoping.

## TD-BUFMGR-DBASE-BUFFERS — seam contract reconcile pending

`dbcommands.c`'s `dbase_redo` (XLOG_DBASE_CREATE_FILE_COPY / XLOG_DBASE_DROP)
calls `FlushDatabaseBuffers(dbid)` and `DropDatabaseBuffers(dbid)` — two
bufmgr.c whole-database shared-buffer operations (scan NBuffers, match by
`RelFileLocator.dbOid`). The bufmgr owner (`backend-storage-buffer-bufmgr`) is
a complete CATALOG unit but its F-decomp did not port these two functions. The
seams `drop_database_buffers` / `flush_database_buffers` are declared on the
bufmgr `-seams` crate so the landed `dbase_redo` consumer can call them; they
loud-panic until bufmgr ports them. Tracked in seams-init's
`CONTRACT_RECONCILE_PENDING`. Pay down by porting DropDatabaseBuffers /
FlushDatabaseBuffers in bufmgr, installing both seams, and deleting the two
allowlist lines.


## backend-backup-manifest: cryptohash error detail text dropped

- **What:** `InitializeBackupManifest` / `SendBackupManifest` /
  `AppendStringToManifest` raise `elog(ERROR, "... : %s", pg_cryptohash_error(ctx))`
  on a `pg_cryptohash_*` failure. The repo's cryptohash subsystem (the
  `common-cryptohash-seams` external primitive) does not expose
  `pg_cryptohash_error`, so the `": %s"` detail suffix is omitted; the leading
  message ("failed to {initialize,finalize,update} checksum of backup manifest")
  is preserved verbatim.
- **Why it's safe:** the SQLSTATE/level and primary message are unchanged; only
  the provider's error-string detail is missing. These paths are unreachable in
  the in-tree SHA-256 software fallback.
- **Fix:** add a `pg_cryptohash_error` seam to `common-cryptohash-seams` when
  the cryptohash owner lands, then append the detail.

## TD-INITSPLAN-REBUILD-JOINCLAUSE (backend-optimizer-plan-analyzejoins)

analyzejoins.c's `remove_leftjoinrel_from_query` (left-join removal, #294) calls
`rebuild_joinclause_attr_needed` (initsplan.c:3559) to re-add the `attr_needed`
bits contributed by join clauses after a join removal cleared the per-rel sets.

- **Owner**: initsplan.c (`backend-optimizer-plan-init-subselect`). This
  function is NOT yet ported there (the sibling `rebuild_lateral_attr_needed`
  IS ported and installed). The seam is declared in
  `backend-optimizer-plan-small-seams` and called from analyzejoins.
- **Status**: `CONTRACT_RECONCILE_PENDING` allowlist entry
  (`backend_optimizer_plan_analyzejoins`, `rebuild_joinclause_attr_needed`);
  loud-panics on a real left-join-removal call path until the owner lands it.
- **Fix**: port `rebuild_joinclause_attr_needed` in init-subselect, install it
  from its `init_seams()`, and delete the allowlist entry.

## TD-ENCNAMES-ICU (backend-utils-mb-mbutils)

`is_encoding_supported_by_icu(encoding)` is declared in
`backend-utils-mb-mbutils-seams` but its logic lives in `common/encnames.c`
(`pg_enc2icu_tbl` + `get_encoding_name_for_icu`, encnames.c:461/472), NOT in
mbutils.c — mbutils.c never references it. The seam was mis-homed into the
mbutils seam crate.

- **Owner**: the encnames unit (unported in the main model). The only consumer
  is `recomputeNamespacePath`'s ICU branch (namespace.c:2323).
- **Status**: `CONTRACT_RECONCILE_PENDING`
  (`backend_utils_mb_mbutils`, `is_encoding_supported_by_icu`). The mbutils owner
  deliberately does NOT install it — wrong-homing the ICU encoding-name table in
  mbutils would violate ownership-by-C-source. Loud-panics on a real ICU-branch
  call path until encnames lands.
- **Fix**: port `pg_enc2icu_tbl`/`is_encoding_supported_by_icu` in the encnames
  unit (ideally relocating the seam declaration to a common-encnames seam crate),
  install it from that owner's `init_seams()`, and delete the allowlist entry.

## TD-FMGR-GETARG-BYREF — by-reference PG_GETARG readers on the nodes frame (RESOLVED for the channel; consumer wiring remains)

RESOLVED (the channel + four readers): the executor call frame
`types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>` now carries a by-reference
argument side channel `ref_args: Vec<Option<FmgrArgRef>>` — the `no_std` mirror
(`FmgrArgRef::Cstring`/`Varlena`) of the `types_fmgr` ABI frame's `ref_args`
(`types_fmgr::RefPayload` is `std`-only, so the `no_std` nodes crate cannot name
it — the WONTFIX dual-home). The four by-reference readers are now INSTALLED in
`backend-utils-fmgr-core::init_seams()`, reading `fcinfo.ref_arg(n)` and
allocating their result in the frame's seeded `fn_mcxt`:

- `pg_getarg_name` (`PG_GETARG_NAME` → `Name` NUL-trimmed text),
- `pg_getarg_text_pp` / `pg_getarg_varlena_pp` (the referent's full varlena image
  copied into the call context),
- `pg_getarg_cstring` (`unknown`-literal `cstring`, leaked into the call context
  as `&'mcx str`).

The live by-reference-ARGUMENT path the executor actually exercises was already
faithful BEFORE this change and is unaffected: the interpreter's
`EEOP_FUNCEXPR[_STRICT]` steps dispatch through the `function_call_invoke_datum`
seam, which builds the **`types_fmgr` ABI carrier** (whose `ref_args` is populated
by `datum_to_ref_arg` from the canonical `Datum::ByRef`/`Cstring`) — so
`length('x'::text)`, `upper`, `||`, `::name`, `avg` etc. already returned. This
TD only ever blocked the handful of builtins that hold the **`types_nodes`
executor frame directly** (`nextval(text)`, foreign `pg_options_to_table` /
`postgresql_fdw_validator`, funcapi `BuildTupleFromCStrings`), which is why
`has_*_privilege(name, ...)` did NOT actually wall (it routes through the
`types_fmgr` carrier).

REMAINING (consumer wiring, not this keystone): those direct-frame consumers are
not yet wired into any dispatch (e.g. `nextval` is a `pub fn` over the nodes frame
with no caller that builds + seeds its frame; `pg_options_to_table` is SRF-blocked
on `split_pathtarget_at_srfs`). When such a consumer is wired, its dispatcher must
seed `ref_args[n]` (via `FunctionCallInfoBaseData::set_ref_arg`) for each by-ref
arg, exactly as the `types_fmgr` `function_call_coll_ref_args` path seeds its
`ref_args` — then the installed reader returns. The reader loud-panics if a by-ref
arg slot was not seeded (a wiring bug, not a data path). `typmodin` is tracked
separately (it needs a constructed `cstring[]` array argument; bpchar/varchar
typmodin functions are also not yet registered).

- **Owner**: `backend-utils-fmgr-core` (readers) + each direct-frame consumer's
  dispatcher (seeding).
- **Status**: channel + four readers INSTALLED; consumer dispatch wiring +
  `ref_args` seeding remain per-consumer.

## TD-FMGR-FN-OID-AND-EXPR-NODE — fn_oid_and_expr drops the call-expression node

The `fn_oid_and_expr` seam returns `(Oid, Option<&Node>)`; the OID reads off the
frame's `flinfo`, but the call-expression node is NOT recoverable as a
frame-borrowed `&Node`. `FmgrInfo.fn_expr` carries the node *erased* as an owned
`Rc<dyn Any>` holding a `primnodes::Expr` (stamped by `fmgr_info_set_expr`); a
`&Node` cannot be borrowed out of that `Expr`, so the seam reports `None` for the
expr. This is faithful for non-polymorphic SRFs (the common case): the funcapi
`internal_get_result_type` consumer only consults `call_expr` to resolve
*polymorphic* result/OUT types, so only a polymorphic-return SRF called without a
recoverable call expr is degraded (it would `ereport` "could not determine
polymorphic type" exactly where C needs the node).

- **Owner**: `backend-utils-fmgr-core` (seam) + the funcapi result-type cluster.
- **Status**: installed, returns `(fn_oid, None)`.
- **Fix**: either carry a `Node` (not bare `Expr`) in the `fn_expr` erased slot so
  a `&Node` can be borrowed, or re-sign `internal_get_result_type` /
  `fn_oid_and_expr` to thread the owned `&Expr` the frame already holds.

## TD-FMGR-RECORD-RECV-CURSOR — record_recv per-column whole-buffer check

`record_column_receive` (fmgr-core) should mirror C's
`if (buf.cursor != buf.len) ereport(ERRCODE_INVALID_BINARY_REPRESENTATION,
"improper binary format in record column %d", colno)` after the element receive
call. The typed `receive_function_call_typed` helper does not surface the number
of bytes the receive function consumed, so the whole-buffer-consumed check is not
performed here; the `colno` parameter is currently unused. A malformed binary
record column that the element receive proc under-reads is not rejected at this
seam (the element proc still rejects genuinely invalid data).

- **Owner**: `backend-utils-fmgr-core`.
- **Status**: installed; conversion faithful, the cursor check omitted.
- **Fix**: thread the consumed-bytes count out of the typed receive helper and
  re-add the `colno`-tagged whole-buffer check.

## TD-JSONFUNCS-FMGR-ARG-DETOAST — jsonfuncs SRF/populate argument detoast

`jsonfuncs.c`'s SQL entry points (`json[b]_object_keys`, `json[b]_each[_text]`,
`json[b]_array_elements[_text]`, `json[b]_populate_record(set)`,
`json[b]_to_record(set)`) read a `json`/`jsonb` varlena argument — and
`populate_recordset` additionally a composite `record` argument — from the fmgr
call frame. The repo's trimmed `FunctionCallInfoBaseData` carries arguments as
bare-word `types_datum::Datum`, and the bare-word -> detoasted `&[u8]` (and ->
`FormedTuple` for the record arg) conversion is the project-wide fmgr
argument-detoast boundary that `backend-utils-fmgr-funcapi` (the call-frame
owner) has not yet grown.

- **Owner**: `backend-utils-fmgr-funcapi` (the SRF/fmgr call-frame unit).
- **Seams** (declared on funcapi, called by jsonfuncs, NOT yet installable):
  `srf_arg_varlena_bytes(mcx, fcinfo, n) -> PgResult<PgVec<u8>>` (the json/jsonb
  arg bytes) and `srf_arg_record(mcx, fcinfo, n) -> PgResult<FormedTuple>` (the
  composite record arg).
- **Status**: `CONTRACT_RECONCILE_PENDING`
  (`backend_utils_fmgr_funcapi`, `srf_arg_varlena_bytes`) and
  (`backend_utils_fmgr_funcapi`, `srf_arg_record`). They loud-panic on a real
  SRF call path until the detoast boundary lands.
- **Fix**: grow funcapi's fmgr argument-detoast accessors (bare-word varlena ->
  detoasted bytes, bare-word composite -> `FormedTuple`), install both seams
  from funcapi's `init_seams()`, and delete the two allowlist entries.

Note (RESOLVED): five seams declared in `backend-utils-adt-jsonfuncs-seams`
(`output_function_call`, `cast_function_call`, `text_datum_bytes`,
`deconstruct_array`, `walk_composite`) are the catalog/fmgr/array/typcache
*halves* of `json.c`'s / `jsonb.c`'s `datum_to_json_internal` /
`array_to_json_internal` / `composite_to_json` (`OidOutputFunctionCall` /
`OidFunctionCall1` (fmgr.c), `TextDatumGetCString` (varlena), `deconstruct_array`
(arrayfuncs.c) + the json element classification, and the inline composite walk
over `lookup_rowtype_tupdesc` + `heap_getattr` + `json_categorize_type`). The
`json.c` porter homed them in the jsonfuncs seam crate (because
`json_categorize_type` is their neighbour) but their declarations omitted the
`Mcx<'mcx>` the real allocating calls require, so they were never installed —
introduced contract debt. **Reconciled:** each decl now carries `Mcx<'mcx>`;
`jsonfuncs` (which already depends on fmgr-core / arrayfuncs / typcache / the
varlena+fmgr+detoast seam crates — the faithful providers, reached without a
cycle because `json`/`jsonb` sit below it) implements them in
`backend-utils-adt-jsonfuncs/src/json_render.rs` and installs all five from its
`init_seams()`. The `json`/`jsonb` consumers thread `mcx` through their
`datum_to_json_internal` / `array_to_json_internal` / `composite_to_json` /
`add_json` helpers. No keystone was needed.

## TD-INDEXCREATE-BOOTSTRAP-LEGS

`catalog/index.c`'s `index_create` / `index_constraint_create` /
`index_set_state_flags` are ported and installed (the CREATE INDEX gate), but
three legs that `index_create` reaches ONLY in bootstrap mode (or via the
deferrable-constraint path) call seams whose owners exist yet cannot install the
seam without a prerequisite keystone, so they stay seam-and-panic
(mirror-pg-and-panic). Each has a `CONTRACT_RECONCILE_PENDING` allowlist line in
`crates/seams-init/src/lib.rs`:

* `backend_bootstrap_bootstrap :: index_register` — the bootstrap owner stores
  the registered index's `IndexInfo` as `IndexInfo<'static>` on its no-gc list,
  but the seam crosses a per-query `IndexInfo<'mcx>`. Installing requires the
  bootstrap-context lifetime keystone (a real `'static` deep-copy of `IndexInfo`
  into the bootstrap IL context). Only reached during initdb bootstrap.
* `backend_utils_cache_relcache :: relation_init_index_access_info` — the
  owner's `RelationInitIndexAccessInfo(&mut RelationData)` runs inside the
  registry build with a `&mut` entry; the relcache exposes no mutable-by-OID
  registry accessor a by-OID seam could use. Needs the registry-mutable-entry
  keystone. Bootstrap-only leg (non-bootstrap rebuilds the entry via the sinval
  flush at `CommandCounterIncrement`).
* `backend_commands_trigger :: create_unique_key_recheck_trigger` — the
  deferrable PK/UNIQUE leg calls `CreateTrigger`, which the trigger manager has
  not ported yet. Only reached for a DEFERRABLE constraint.

Delete each entry + this section's bullet when its owner installs the seam.

## appendinfo: find_base_rel_ignore_join probed in-crate (relnode owner un-seamed)

`backend-optimizer-util-appendinfo`'s `find_appinfos_by_relids` needs C's
`find_base_rel_ignore_join(root, i)` (relnode.c) to distinguish an outer-join RT
index (skip) from a base rel missing its `append_rel_array` entry (error). relnode
owns it but exposes NO seam for it, and relnode depends on appendinfo (a direct
call would cycle), so appendinfo reimplements the observable distinction as an
in-crate `simple_rel_array` slot probe (in-range `None` = OJ → skip; in-range
`Some` = base rel → caller errors; out-of-range = panic, matching the C terminal
`elog`). The real relnode impl additionally reads the RTE kind for a debug
assertion, which needs `run` that the `find_appinfos_by_relids` seam contract does
not carry. Faithful for every reachable input; only the debug-assert RTE-kind
check is dropped. Remove when relnode exposes a `find_base_rel_ignore_join`-shaped
seam (and `find_appinfos_by_relids` is re-signed to carry `run`).

## TD-VARLENA-HEADER-CONVENTION: by-ref Datum carries TWO varlena header conventions (canonical header-FUL vs fmgr-core header-LESS); the boundary bridges translate via a structural heuristic, not the principled header-ful-everywhere model — PARTIAL

- **Location:** `crates/backend-utils-fmgr-core/src/lib.rs` (the bridge marshallers
  `datum_to_ref_arg`, `tuple_value_to_arg`, `oid_output_function_call_datum_seam`,
  `byref_payload_for_typlen`, and the helper `byref_to_headerless_payload`);
  `crates/types-datum/src/varlena.rs` (`varsize_4b_of`); and every adt function core
  (string/numeric/array/json/bytea) that builds/consumes a header-LESS `VARDATA` payload.
- **Description:** A by-reference varlena value has two byte-layouts in the tree. The
  canonical/heap `Datum::ByRef` is HEADER-FUL (`[4-byte VARSIZE][content]` — what
  heap_form/deform_tuple and the array on-disk code read). The fmgr `RefPayload::Varlena`
  lane is HEADER-LESS (`[content]` — what the adt cores access, mirroring C `VARDATA`).
  So every by-ref value crossing the fmgr boundary must have its 4-byte header *stamped*
  (result → heap) or *stripped* (arg → core). C has NO such split: one header-ful
  representation everywhere, content read via `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` macros (no
  boundary conversion), with header variants normalized by `PG_DETOAST_DATUM` and the
  self-describing `VARATT_IS_1B/4B/COMPRESSED/EXTERNAL` tag bits. The split is a port
  artifact (the cores were ported to take a header-less content slice).
- **Current state (fix landed `f482afff7`):** a boot-safe STRUCTURAL heuristic — strip the
  header iff the image is a self-consistent 4-byte varlena (`VARSIZE_4B == len`); a
  fixed-by-ref `name` buffer or a still-header-less parser `Const` fails the check and
  passes verbatim. This was chosen because the principled fix — thread `typlen` from
  `pg_type` at each bridge — STACK-OVERFLOWS at boot: type input functions run before the
  catalogs are built, so the catalog lookup recurses into the pg_type self-build.
- **Limitations of the heuristic:** (a) handles ONLY the flat 4-byte header — NOT the 1-byte
  SHORT varlena header or TOAST'd values (compressed / out-of-line external); (b) a
  vanishingly-rare collision (a header-less payload whose first 4 bytes happen to equal its
  own length would be wrongly stripped); (c) composite/record returns (`pg_input_error_info`)
  and the UPDATE/DELETE `ctid`-junk fetch route through paths that still need the same care.
- **Container-type exception (fix landed after the strip regressed arrays):** the strip
  heuristic was over-broad — it stripped EVERY self-consistent 4-byte varlena, including the
  CONTAINER types (array / composite-record / range / multirange) whose adt I/O cores read the
  FRAMED (header-ful) image directly (e.g. `array_out` reads `ARR_ELEMTYPE` at byte 12 of the
  `ArrayType`). Stripping shifted the array image left by `VARHDRSZ` and made `array_out` read
  the dim count as the element type (`SELECT ARRAY[1,2,3]` → `cache lookup failed for type 3`).
  Fix: `arg0_consumes_header_ful_varlena` / `type_adt_core_is_header_ful` (fmgr-core) detect
  these types from the I/O function's first arg type and pass the `ByRef` referent VERBATIM —
  in `oid_output_function_call_seam` / `oid_send_function_call_seam` (via
  `tuple_value_to_arg_verbatim`) and in `proc_arg_typlens` (via the `HEADERFUL_VERBATIM_TYPLEN`
  sentinel for the canonical-`Datum` invoke paths). Safe on the output/arg path (post-bootstrap,
  catalogs exist). This is a symptom of the same split — the principled header-ful-everywhere
  end-state subsumes it (no per-core convention divergence to reconcile).
  NOTE: the inner ELEMENT header convention for varlena-element arrays (e.g. `text[]`) is a
  SEPARATE, pre-existing bug (`SELECT ARRAY['a','b']` → `range end index 47 out of range`,
  failing identically before and after the strip commits) — NOT part of this regression.
- **Intended fix (principled end-state):** UNIFY ON HEADER-FUL EVERYWHERE, matching C. Make
  the adt cores work with the framed varlena via `VARDATA_ANY`/`VARSIZE_ANY_EXHDR`-style
  accessors + detoast-at-entry, which DELETES the translating bridges entirely and gets
  short-header + TOAST handling for free. This is a large, mechanical, per-core fan-out
  across every string/numeric/array/json/bytea crate (~hundreds of functions) — an ideal
  WORKFLOW campaign. Principle (same as the node-opaque downcast): let the value's own
  structure be the witness, never an external catalog lookup. Until then, the heuristic is
  the pragmatic 80% that unblocks the common cases.

## `transformIndexConstraint` NO-INHERIT not-null check is local-only

- **Where:** `backend-parser-parse-utilcmd/src/index_constraint.rs`
  (`transform_index_constraint_catalog`, installed behind the
  `transformIndexConstraintCatalog` outward seam).
- **What:** C scans `cxt->nnconstraints` (every accumulated not-null constraint,
  including those `transformColumnDefinition` already added for an explicit
  `NOT NULL` column) to reject a conflicting `NO INHERIT` declaration when a PK
  column is already not-null. The seam receives only `columns`, not the
  `nnconstraints` accumulator, so this port scans only the not-null constraints
  it adds in the same call.
- **Impact:** common case (`a int PRIMARY KEY`, column not yet not-null) is
  exact. Only the edge case of an explicit `NOT NULL ... NO INHERIT` column that
  is also a PK key would miss the conflict error.
- **Fix:** widen `transformIndexConstraintCatalog` to also carry the in/out
  `nnconstraints` accumulator (it already returns `extra_nn`), then scan the
  full list as C does.

## TD-STATIC-EROSION: `'static`-erased arena lifetimes are the root cause of the recurring cross-context use-after-free / wrong-context-clone bug class

- **Location**: tree-wide, but concentrated at three "islands": plancache
  (`crates/backend-utils-cache-plancache`), portals
  (`crates/types-portal`, `crates/backend-utils-mmgr-portalmem`,
  `crates/backend-tcop-pquery`), and every **seam** boundary (the installed
  `FN`-pointer dispatch in `*-seams` crates). Node carriers (`types-nodes`)
  carry `Mcx<'static>` / `PgBox<'static, _>` / `PgVec<'static, _>` markers.
- **Symptom (a whole bug class, several already fixed individually)**:
  - cross-context **use-after-free** at teardown: `BuildCachedPlan` qlist drop
    after `transient` freed (`589c87ef7`); portal `portalContext` moved by
    `Option::take` dangling `'static` stmt markers (`0b7c45779`); tuplesort
    `MinimalTuple` stored from a sort context freed by `tuplesort_end`
    (`287405b94`).
  - **wrong-context `clone()`**: a plain `Clone` reproduces a node into the
    **source** context (the `Mcx` allocator handle is `Copy` and reused) while
    the markers claim `'static`; the copy then dangles when the source arena
    resets. Surfaces as a `.clone()`-where-`.clone_in(mcx)`-was-needed bug,
    repeatedly (Aggref in HAVING OR-quals `d88a3b8be`; `Node::Expr` arm of
    `Node::clone_in` routing `SubLink`/`Aggref` through shallow `clone`
    `622a7cf9c`). The context-bearing variants make derived `clone()` PANIC as
    a tripwire precisely to force `clone_in`.
  - **scratch-context arena-escape**: a "switch to a fresh `MemoryContext`, do
    work, drop it" wrapper passes that throwaway `mcx` into a callee that
    interns nodes ESCAPING into a longer-lived arena. selfuncs `call_oprrest`/
    `call_oprjoin` (`backend-utils-adt-selfuncs/src/dispatch.rs`) created a
    per-call `MemoryContext::new("selfuncs restriction estimate")` and passed it
    to `examine_variable`, which sets `vardata.var = root.alloc_node(...)` into
    `PlannerInfo::node_arena`. When the throwaway context dropped at return, the
    arena held dangling `Box<Expr, Mcx>` children that under-charged their freed
    context at planner teardown (`drop PlannerInfo::node_arena` in
    `standard_planner`) → `uncharging N with only 0 charged`. Manifested as a
    crash on any `GROUP BY ... HAVING agg(col) <op> ...` (an aggregate with
    non-empty `args` in the HAVING qual; `count(*)` has empty args so never
    recursed into a boxed child and silently "worked"). **Fix**: use the planner
    context (`run.mcx()`) for these estimators, matching C, which runs them in
    `CurrentMemoryContext` — never a throwaway. Lesson: a scratch context is
    only safe if NOTHING it allocates escapes the call (`cost_qual_eval_walker`'s
    scratch is fine — it only detoasts in-scope); the moment a callee can
    `alloc_node`/intern, the scratch must be the planner/long-lived context.
- **Root cause**: the port models C MemoryContexts (runtime-scoped arena
  allocators with no per-object destructors) under Rust ownership (objects DO
  have destructors that reach back into the arena). Correctness of **drop
  order** and **clone target context** thus becomes a property the type system
  would normally enforce via lifetimes — but the relevant lifetimes are
  **erased to `'static`**, so nothing is enforced. The erasure is partly
  **forced** (self-referential `Portal` owning its context + pointing into it;
  backend-lifetime aliased `CachedPlanSource`; runtime `MemoryContextDelete`
  has no lexical region) and partly **avoidable/contagious** (`'static` used as
  the path of least resistance where a real `'mcx` arena lifetime was
  achievable; one `'static` field spreads to enclosing structs). A major
  structural driver of the avoidable share is the **seam architecture**: an
  installed `fn`-pointer cannot carry the caller's lifetime, so every value
  crossing a seam has its lifetime erased by construction.
- **Why it keeps recurring**: each faithful translation of "switch to a scratch
  context, do work, delete it" reproduces C's control flow, which was only safe
  *because C had no destructors*. The Rust objects created in that window have
  destructors that outlive the C-intended teardown → latent UAF. Because the
  markers are `'static`, the compiler cannot flag it; it shows up as
  nondeterministic `SERVER_DIED` crashes (allocator-timing dependent), which is
  what makes long shared-cluster regression runs untrustworthy.
- **Fix direction (in priority order)**:
  1. **Claw back the avoidable `'static`.** Wherever a node provably lives for
     exactly one planner run, type it `Node<'mcx>` tied to that run's arena so
     the borrow checker rejects `clone()`-into-wrong-context and
     drop-after-free *at compile time*. Highest leverage: tighten the **seam
     boundary** to carry an `'mcx` where the owner has one (most avoidable
     `'static` is manufactured at seam crossings).
  2. **For the forced islands** (plancache, portals, seams), keep manual
     discipline but make it structural: `Box` any movable context so its
     address is stable across `Option` moves (the `0b7c45779` pattern); force
     deep-copy across context boundaries via `clone_in(target)` not `clone()`;
     order drops so arena-resident destructors run before the arena frees.
  3. **Lint/tripwire**: extend the derived-`Clone`-panics-on-context-bearing-
     variants pattern, and consider a grep/CI guard flagging new `.clone()` on
     node/Expr carriers (should be `.clone_in`).
- **Status**: keystone-class, cross-cutting. Individual UAF/clone instances are
  being fixed as found; the structural fix (seam-carried `'mcx`) is the durable
  resolution and has not been attempted. Related: the by-ref Datum header-ful
  flip is a *different* keystone (varlena representation), but shares the theme
  of an invented convention that the C model didn't have.

## Parallel vacuum gated to serial (cross-process DSM not yet ported)

- **Location**: `crates/backend-commands-vacuumparallel/src/lib.rs`,
  `parallel_vacuum_compute_workers` (mirrors `vacuumparallel.c:548`).
- **Description**: Parallel vacuum is gated to serial. `vacuumparallel.c`'s
  shared state (`ParallelVacuumState` / the shared per-index stats DSM segment)
  currently lives in process-private / thread-local memory that real `fork(2)`
  workers cannot inherit. When `VACUUM` launches parallel index-vacuum workers,
  the workers can't see the leader's shared vacuum state, so the leader hangs
  waiting for them (or workers error with `relid = 0`). To avoid the hang,
  `parallel_vacuum_compute_workers` returns `0` (as if
  `max_parallel_maintenance_workers = 0` / parallelism unavailable — exactly
  C's "parallelism disabled" path), so any `VACUUM (PARALLEL n)` still parses
  but degrades to serial.
- **Faithfulness**: VACUUM output is identical serial vs parallel — parallel
  index vacuum is a pure performance optimization that splits index-vacuuming
  across workers; rows, stats, and results are the same. So the gate introduces
  no output diffs. C itself runs vacuum serially when
  `max_parallel_maintenance_workers = 0`, when there are too few indexes, or
  when the parallel infrastructure is unavailable.
- **Intended fix**: Port `ParallelVacuumState` / shared index stats off
  process-private/thread-local memory onto a genuine cross-process DSM carrier
  (the `ShmemInitStruct` / DSM-carrier class shared by the other shmem
  keystones). Then remove the early `return Ok(0)` gate and the
  `#[allow(unreachable_code)]`; the faithful worker-count computation is
  retained directly below the gate so it can be re-enabled unchanged.
- **Status**: deliberate interim gate; perf-only deferral.

## The default `io_method = worker` is unported — the server requires `io_method = sync` to boot, which every harness must inject

- **Location**: `crates/backend-storage-aio-methods/src/lib.rs:130`
  (`DEFAULT_IO_METHOD = IOMETHOD_WORKER`, faithfully matching C's
  `storage/aio.h`) and `pgaio_worker_ops()` (~line 561), whose
  `shmem_size`/`shmem_init`/`init_backend`/`submit`/`wait_one` all `panic!`
  with "the worker IO method (method_worker.c) is not yet ported (task #15 F4);
  run io_method = sync".
- **Description**: PostgreSQL 18 defaults `io_method` to `worker`. The worker
  AIO method (`method_worker.c`, the io-worker subprocess queue) is not yet
  ported, so a postmaster started with the default `io_method` panics in
  `AioShmemSize`/`AioShmemInit` during startup — before it ever opens a socket.
  The synchronous IO method (`io_method = sync`) is fully ported and is the
  only method this port supports today. Therefore **every** way of launching the
  server must pass `io_method = sync` (the main regress harness does this with
  `-c io_method=sync` on the postmaster command line — see `measure.sh` /
  `/private/tmp/qmeasure/run_schedule.sh`).
- **Impact on TAP suites**: PostgreSQL's `prove` + `PostgreSQL::Test::Cluster`
  harness (recovery / archive / streaming tests) does *not* pass postmaster
  command-line GUCs, so under TAP defaults the pgrust postmaster panics at
  startup and the test reports `poll_query_until timed out` /
  `No postmaster PID` — looking like a recovery bug when it is purely the boot
  gate. The fix is to inject `io_method = sync` (plus a larger
  `max_stack_depth`) via the standard `$TEMP_CONFIG` env hook that Cluster.pm
  appends to every node's `postgresql.conf`. `scripts/run-recovery-tap`
  does exactly this, and with it the entire archive-recovery group
  (002/003/020/023/024/025/042/045) passes 45/45 with no code change — the
  archive-recovery WAL-replay path (`xlogarchive.c` / `xlogrecovery.c`'s
  `RestoreArchivedFile` / `WaitForWALToBecomeAvailable` /
  `XLogFileRead{,AnyTLI}` / `rescanLatestTimeLine`) is already complete and
  correct.
- **Intended fix**: port `method_worker.c` (the io-worker subprocess pool +
  its shmem submit/completion queue, the `IoMethodOps` for `IOMETHOD_WORKER`),
  then the default boots unmodified and the `pgaio_worker_*` panics are removed.
- **Status**: faithful "unported method" boundary; harnesses inject
  `io_method = sync` until the worker method lands.

## TD-HASHJOIN-BATCHCXT: `HashJoinTableData::batch_mcx` transmutes a `Mcx` borrow of a self-owned boxed context to the table's `'mcx` (erased-handle self-reference)

- **Location:** `crates/_support/types/nodes/src/nodehash.rs:536` (`HashJoinTableData::batch_mcx`), the `unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'mcx>>(ctx.mcx()) }`. Backing field: `batch_cxt: Option<Box<::mcx::MemoryContext>>` (`:503`); reset via `reset_batch_cxt` (`:545`). Landed `ba3d707d6` ("give the hash table a self-owned bump batchCxt, reset per batch wholesale").
- **Description:** to mirror C's `ExecHashTableReset` (one wholesale `MemoryContextReset(batchCxt)` per batch instead of per-tuple `Vec::clear` + N `Drop`s — measured ~16,000 per-chunk frees → 0 on a 500-tuple×16-batch cycle), the hash table must *own* its batch context AND hold per-batch arenas (`tuples`/`skew_tuples`/`chunk_arena`) that allocate into it. That is the self-referential-struct pattern the borrow checker forbids (a `PgVec<'mcx>` field borrowing a sibling field). The chosen workaround is the `erh_table`-style **erased handle**: the context lives behind a heap `Box<MemoryContext>` (stable address that survives moves of the *table*), and `batch_mcx()` reborrows `ctx.mcx()` and **transmutes its lifetime up to the table's `'mcx`** so the arenas can name it as their allocator. The `'mcx` is a lie the compiler can't verify; it is made *sound at runtime* only by two hand-maintained invariants: (1) `batch_cxt` is the table's **last field**, so drop order destroys the borrowing arenas before the context they point into; (2) `reset_batch_cxt` only resets once every arena allocation has been released (the re-entrant probe loop saves an index `hj_CurTuple`, not a live borrow, so no borrow outlives the reset). Get either wrong → use-after-free. This is the SAME unsafe class as `McxOwned` (whose Stacked/Tree-Borrows soundness we fixed in `5a71ecbd6`) and the trigger-firing/`erh_table` `'static` markers — a struct owning a context it allocates into. Serial path only; the parallel-hash path leaves `batch_cxt = None` (tuples live in DSA/shared tuplestores), and `batch_mcx` falls back to the per-query context.
- **Verification at landing:** `join`/`join_hash` + forced multi-batch (`work_mem=64kB`) + rescan + skew + outer-join all 0-diff; `cargo +nightly miri test -p mcx` clean incl. 3 new self-owned-arena/rebatch/churn tests. So it is *currently* sound — the debt is that soundness rests on the drop-order + reset-timing invariants, not on the type system.
- **Fix (preferred):** once a borrow-checked self-owned-arena abstraction exists (e.g. a hardened `McxOwned`-style wrapper that the `5a71ecbd6` exposed-provenance work makes Miri-clean, or a dedicated `OwnedBatchContext` type that encapsulates the box + reborrow + drop-order so the `transmute` lives in ONE audited place with a safe API), route `batch_mcx`/`batch_cxt` through it and delete the ad-hoc `transmute` here. That converts the per-site hand-checked invariant into one centrally-audited unsafe boundary. Until then: treat the field order of `HashJoinTableData` and the `reset_batch_cxt` call sites as load-bearing — do not reorder fields, and do not reset while any arena borrow is live.
- **Related:** [[the `alloc_node`/`erase_lifetime` UAF class above]]; the WONTFIX two-`FunctionCallInfoBaseData`-homes entry; `erh_table` (`crates/pl/plpgsql/src/exec/src/erh_table.rs`) and the trigger-firing `'static`-marked headers use the identical erased-handle pattern.

## TD-PGTRGM-INDEX-REMAINDER: only the pg_trgm REGEXP index strategy (`~`/`~*`) remains unported — the `trgm_regexp.c` NFA engine (shared by GIN + GiST)

- **Location:** `crates/contrib/pg_trgm/src/lib.rs` `fc_gin_extract_query_trgm` / `fc_gin_trgm_consistent` / `fc_gin_trgm_triconsistent` (`unported_index_symbol("... (regexp strategy)")`) and `crates/contrib/pg_trgm/src/trgm_gist.rs` `gtrgm_consistent` (`unported_regexp(...)`) — both raise a clean error for `REGEXP_STRATEGY` (5) / `REGEXP_ICASE_STRATEGY` (6). Every NON-regexp strategy is ported and wired.
- **What IS done (the keystones):** BOTH the generic GIN dispatch (`gin-core-probe::extdispatch` over `::gin::extproc`) AND the generic GiST dispatch (`gist-proc::extdispatch` over `::gist::extproc` — built this pass, mirroring GIN: re-resolve `proc_oid` via `fmgr_info`, invoke through a real fmgr frame, marshal `GISTENTRY`/`GistEntryVector`/`GIST_SPLITVEC` + the `*recheck`/`*penalty`/`*size` out-params as key-image byte protocol structs on the internal lane, the consistent/distance query on the by-ref lane) reach ANY registered opclass support fn. pg_trgm's `gin_trgm_ops` AND `gist_trgm_ops` Similarity / Word / Strict-Word / Like / ILike / Equal (+ GiST `<->` Distance KNN) strategies are fully ported (`trgm_gist.rs` ports the TRGM signature-bitmap logic + every `gtrgm_*` body 1:1) and the built-in GIN/GiST opclasses keep their fast typed arms (box/polygon/range/tsvector GiST + anyarray/tsvector/jsonb GIN regress = 0 diff).
- **Remainder — REGEXP (`~`, `~*`), GIN AND GiST:** needs `trgm_regexp.c` (~2360 lines): `createTrgmNFA` (regex → trigram NFA, the `TrgmPackedGraph`) + `trigramsMatchGraph`. For GIN the graph rides `extra_data` (the protocol already carries it); for GiST `gtrgm_consistent` builds `check` via `trgm_presence_map` (also unported) and a sign-bitmap probe, then `trigramsMatchGraph`. The dispatch substrate is complete for both — this is a pure body port, no new dispatch infra.
- **`siglen` opclass option (faithful divergence):** the owned GiST dispatch does not thread the `siglen` reloption to the support procs (the same documented `tsvector_ops` divergence); `trgm_gist.rs` uses `SIGLEN_DEFAULT` (12) on the build side and reads the stored signature's own length on the read side. Exact for the default index (the regress tests); only an index built with an explicit non-default `siglen` differs in physical signature length (queries recheck on the heap, so results stay correct).
- **Intended fix:** port `trgm_regexp.c` (+ `trgm_presence_map`) and wire the regexp arms in both `lib.rs` (GIN, over `extra_data`) and `trgm_gist.rs` (GiST). Self-contained body port on the now-complete generic GIN+GiST dispatch substrate.

## TD-SEMAPHORE-LEAK-ON-SIGKILL: SysV semaphore sets accumulate unboundedly when postmasters are SIGKILLed across MANY distinct datadirs (the per-key stale-reclaim never fires for keys no future postmaster reuses)

- **Observed:** during a heavy multi-agent / recovery-TAP session, `ipcs -s` showed **~2665 leaked SysV semaphore sets** owned by us. macOS `kern.sysv.semmni` is 87381, so this did NOT exhaust the limit or cause "too many clients" *this* time — but it grows without bound and would bite on a tighter `semmni` (default Linux `SEMMNI` is 128).
- **Location:** `crates/backend/port/sysv/sema/src/lib.rs`. The clean-shutdown removal IS fixed (the postmaster `release_semaphores` `IPC_RMID`s its sets at genuine final `proc_exit`, commit `0e1b7688c`). A stale-reclaim path also exists: `PGReserveSemaphores`/the create loop zaps a leftover set when its `PG_SEMA_MAGIC` matches and its creator PID is dead.
- **Description / why it still leaks:** two gaps. (1) **SIGKILL** of a postmaster (which the agent/test harnesses do constantly — `kill -9`, removed worktrees, crashed lanes) leaves its sem sets behind: the kernel does NOT reclaim SysV semaphores on process death (only an explicit `IPC_RMID` or reboot does). This matches C postgres, which also only reclaims via its recorded control state on a same-datadir restart. (2) The stale-reclaim is **keyed per-datadir-inode** (`next_sema_key = statbuf.st_ino`) and only fires when a NEW postmaster boots over the SAME datadir and hits that key. A fleet that SIGKILLs servers across **thousands of distinct throwaway datadirs** (each test/agent gets a fresh `mktemp` datadir → fresh inode → fresh key) leaves each datadir's orphaned sets unreachable: no future postmaster ever reuses those keys, so they are never zapped → monotonic accumulation under churn.
- **Impact:** benign on macOS's huge `semmni`; a real hazard on default Linux/container `SEMMNI` (128) and for long-lived dev hosts running many crashing clusters — eventually `semget` fails `ENOSPC` and no new postmaster can start. Also pollutes `ipcs` and is a slow resource leak.
- **Investigate / candidate fixes:** (a) a startup or periodic **global stale-sem sweep** that scans ALL `PG_SEMA_MAGIC`-tagged sets (not just the current key) and `IPC_RMID`s those whose creator PID is dead — more aggressive than C, matching the "single durable batch" divergence pgrust already took for sema management; weigh the cost/races of scanning all of `ipcs`. (b) Ensure test/agent **harnesses tear down cleanly** (SIGTERM not SIGKILL; or an `ipcrm` sweep between runs) so the existing clean-shutdown path runs. (c) Consider a POSIX-sema or futex-based backend (no kernel-persistent objects) for the dev/wasm path. Lowest-risk first step: harness-side `ipcrm` sweep + prefer SIGTERM; the principled fix is the global PID-liveness sweep.
- **Related:** the sema-leak clean-shutdown fix (`0e1b7688c`); the COW-private-vs-shmem substrate family (PgArchData, WalRcvData, ReplicationSlotCtl all moved to shmem this session).
