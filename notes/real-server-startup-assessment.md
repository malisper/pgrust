# Real server startup assessment

Tracks the multi-process postmaster standup critical path toward a live
`psql SELECT 1`.

## Postmaster invocation (verified)

Build the binary:

```
CARGO_BUILD_JOBS=10 PGRUST_PGSHAREDIR=/tmp/pgrust_share cargo build --bin postgres
```

Fresh fixture (NEVER reuse a booted dir — always `cp -R` a clean template or
re-`initdb`):

```
D=/tmp/pgrust_fix_$(date +%s)_$$
/tmp/pgrust_pginstall/bin/initdb -D $D --no-locale --encoding=UTF8 -U postgres
```

Launch the multi-process postmaster backgrounded. NOTE: `io_method` MUST be
`sync` — the default `worker` IO method (method_worker.c) is unported (task #15
F4) and `pgaio_worker_shmem_size` panics at startup.

```
SOCK=/tmp/pgrust_sock_$$ ; mkdir -p $SOCK
target/debug/postgres -D $D -k $SOCK -p 54400 \
    -c listen_addresses='' -c io_method=sync -c max_stack_depth=7000 \
    > /tmp/pm.log 2>&1 &
```

Then `psql -h $SOCK -p 54400 -U postgres -d postgres -c 'SELECT 1'`.

## WALL 1al — CLEARED

`SELECT relname FROM pg_class WHERE relname = 'pg_type'` plans to a unique-index
path on `relname` and crashed on the uninstalled
`create_indexscan_plan` seam (`create_scan_plan -> create_plan_recurse`).
Ported `create_indexscan_plan` faithfully into
`backend-optimizer-plan-createplan` (covers both `T_IndexScan` and
`T_IndexOnlyScan`): the `IndexClauses` → (`stripped_indexquals`,
`fixed_indexquals`) extraction via new `fix_indexqual_references` /
`fix_indexqual_clause` (wired over the existing
`backend-optimizer-util-vars::fix_indexqual_operand`), `fix_indexorderby_references`,
the qpqual filter (`is_redundant_with_indexclauses` /
`contain_mutable_functions` + `predicate_implied_by`), nestloop-param
replacement, ORDER BY sort-operator lookup
(`get_opfamily_member_for_cmptype`), index-only `resjunk` marking, and
`make_indexscan` / `make_indexonlyscan`. Seam installed in `init_seams`.

## WALL 1am — CLEARED

`scan_scanrelid` (execUtils.c, `ExecAssignScanProjectionInfo`) panicked: "plan
is not a Scan node: IndexScan(...)". The match had an `IndexOnlyScan` arm but no
`IndexScan` arm. Added `Node::IndexScan(s) => s.scan.scanrelid` (faithful to the
C unconditional `(Scan *)` cast — `IndexScan` embeds `Scan`).

## WALL 1an — CLEARED

`get_op_opfamily_properties` raised `ERROR: operator 93 is not a member of
opfamily 1989` during `ExecIndexBuildScanKeys`. The `rd_opfamily` /
`rd_opcintype` relcache seam bodies (relcache `seams.rs`) indexed
`rd.rd_opfamily[attno]` instead of `[attno - 1]`, so column 1 (`relname`,
name_ops, family text_ops 1994) read column 2's slot (`relnamespace`, oid_ops,
family 1989). All four callers (nodeIndexscan `varattno`, sortsupport
`ssup_attno`, catalog-index `i + 1`, hash `1`) pass 1-based `attno`, matching
the C `rd_opfamily[attno - 1]`. Fixed both bodies to subtract 1.

With 1al-1an cleared, `SELECT relname FROM pg_class WHERE relname = 'pg_type'`
returns the row end-to-end via the index scan; `WHERE oid = 1259` (oid index)
and other single-qual index scans also work. Non-regression: `SELECT 1+1`→2,
`SELECT relname FROM pg_class`→415 rows, multi-connection all hold.

## WALL 1ao — CLEARED

`SELECT relname FROM pg_class WHERE relname = 'pg_type' AND relnamespace = 11`
(a two-qual scan whose WHERE combines via an AND `BoolExpr`) raised `ERROR:
unrecognized node type: 21` (T_BoolExpr = 21). The diagnosis in the prior lane
(an execExpr `EEOP_BOOL_*` gap) was WRONG: execExpr's `ExecInitExprRec` BoolExpr
arm (and the interpreter `EEOP_BOOL_AND/OR/NOT_STEP*` arms) were already ported
and correct. The real gap was in the PARSER: `transformExprRecurse`
(`backend-parser-parse-expr`) had no `Node::BoolExpr` arm. The C grammar
(`gram.y`, via the FFI converter `backend-parser-gram-core/src/convert.rs`)
emits a raw `Node::BoolExpr(rawexprnodes::BoolExpr)` (children = `Vec<NodePtr>`,
untransformed) for `a AND b`, but the dispatcher only handled the cooked
`Node::Expr(Expr::BoolExpr)` shape (and `transformBoolExpr` took a
`primnodes::BoolExpr`), so the raw node fell through to the bare
`elog(ERROR, "unrecognized node type: %d")` default.

Fixed (commit 1): added the `Node::BoolExpr(a) => transformBoolExpr(pstate, a)`
arm to `transformExprRecurse`; re-signed `transformBoolExpr` to take the raw
`rawexprnodes::BoolExpr<'mcx>` (iterate `args: Vec<NodePtr>`, `boxed_node` each
child into `transformExprRecurse`, `coerce_to_boolean`, then
`makeBoolExpr(boolop, args, a.location)` — now passing the real `a.location`,
matching C `transformBoolExpr` parse_expr.c:1412); and turned the now-dead
`Expr::BoolExpr` arm in `transform_expr_node` into an
"unexpected already-analyzed BoolExpr" guard (mirrors the `Expr::SubLink` guard).

That uncovered two planner-side selectivity-estimation gaps for the AND qual
(commit 2): `NumRelids(root, clause)` and `is_pseudo_constant_clause{,_relids}`
(all clauses.c) were declared in `backend-optimizer-path-small-seams` but
installed by NOBODY (they panicked `seam not installed` during
`clauselist_selectivity` of the BoolExpr's conjuncts). Installed the root-aware
`NumRelids` in `backend-optimizer-util-vars` (`seam_num_relids_root`:
`bms_num_members(bms_difference(pull_varnos(root, clause),
root->outer_join_rels))`, faithful to C — the existing `vs::num_relids` rootless
ride could not subtract `outer_join_rels`); and `is_pseudo_constant_clause`
(+`_relids`) in `backend-optimizer-util-clauses` (the C-source owner) over the
existing `grounded::` impls.

Result: the two-qual AND query now returns its 1 row (`pg_type`) end-to-end.

## WALL 1ap — CLEARED (nodeIndexscan array-key expansion widened to canonical by-ref Datum)

`SELECT relname FROM pg_class WHERE relname = 'pg_type' OR relname = 'pg_class'`
first needed the restrictinfo OR-clause builders (commit 3): `indxpath.c`'s
OR-index-path construction rode `make_simple_restrictinfo` /
`make_plain_restrictinfo` (restrictinfo.h macro + restrictinfo.c), declared in
`backend-optimizer-util-restrictinfo-seams` but installed by nobody. Installed
both in `backend-optimizer-util-joininfo` (the restrictinfo.c owner) over the
already-ported `make_restrictinfo` / `make_plain_restrictinfo` bodies
(`make_simple_restrictinfo` = the macro defaults `true,false,false,false,0,
NULL,NULL,NULL`; both resolve the seam's `NodeId` clause to the arena `Expr`).

The prior diagnosis ("by-ref text in the index array-key path is a keystone")
was the BOUNDED 1ah-class shim-edge fix the architect identified, now landed.
The non-searcharray index array-key expansion (`ExecIndexEvalArrayKeys`,
`exec_index_eval_array_keys_into`, nodeIndexscan/src/lib.rs) deconstructed the
`col = ANY(array)` array via two pinned `DatumWord` shim bridges
(`canonical_to_shim_datum` → `array_get_elemtype` / `deconstruct_array`), whose
`byval_word()` collapse panicked at `backend_access_common_heaptuple.rs:139` on
the by-ref text-array varlena.

Fix (bounded, faithful — the canonical by-ref machinery already existed and was
registered): widened the expansion onto the canonical `Datum<'mcx>` lane.
`exec_index_eval_array_keys_into` now reads the array's `Datum::ByRef` bytes
(`arraydatum.as_ref_bytes()`) and calls the value-carrying byte-image seams
`arrayfuncs::array_get_elemtype_bytes` + `deconstruct_array_values_bytes`
(construct.rs, registered arrayfuncs lib.rs:105/111, already used by genam
decode + lsyscache stats), producing real `(Datum<'mcx>, bool)` element pairs.
`IndexArrayKeyInfo.elem_values` (types-nodes nodebitmapindexscan.rs:49) was
re-typed from the by-value shim `types_datum::datum::Datum` to canonical
`PgVec<'mcx, Datum<'mcx>>`; `exec_index_advance_array_keys_into` reads the
canonical element directly (threaded an `mcx` for the per-element `clone_in`);
`sk_argument` (already canonical) is assigned the element straight. The two
`canonical_to_shim_datum`/`shim_to_canonical_datum` bridges were deleted; both
call sites are converted. Contained to nodeIndexscan + the (pre-existing)
arrayfuncs byte-image seams — no new seam, no carrier re-sign.

Verified: the array-key expansion no longer panics on by-ref text; non-regression
holds (single-qual index `WHERE relname = 'pg_type'` → `pg_type`; AND-qual
`… AND relnamespace = 11` → `pg_type`; 415-row seqscan; `SELECT 1+1` → 2;
second connection).

## WALL 1aq — CLEARED (execExpr EEOP_FUNCEXPR by-ref arg-gather, bounded #324 slice)

`SELECT relname FROM pg_class WHERE relname = 'pg_type' OR relname = 'pg_class'`
plans to a **seqscan with the OR `BoolExpr` as a filter qual**; `ExecQual` runs
the `texteq(relname, 'pg_type')` comparisons through the execExpr interpreter's
`EEOP_FUNCEXPR` step, whose call-frame arg-gather (`func_step_inputs` →
`word_of(&c.value)` = `v.as_usize()`, eval_scalar.rs) collapsed the canonical
by-ref `name`/`text` column value to a bare word → panicked `Datum: scalar
accessor called on a by-reference value` (`backend_access_common_heaptuple.rs:139`).

This was the architect's BOUNDED 1ah/1ap-class shim-edge fix (NOT the tree-wide
model campaign the prior framing claimed): the canonical by-ref `Datum<'mcx>`
fmgr lane already existed and was registered (the BRIN `*_coll_datum` seams +
`fmgr_call_seam`'s `datum_to_ref_arg` / `function_call_coll_ref_args_out` /
`ref_out_to_datum` assembly). The fix is assembly over those parts:

1. **New N-ary canonical-Datum fmgr seam.** Declared
   `function_call_invoke_datum(mcx, fn_oid, collation, args: &[Datum<'mcx>]) ->
   (Datum<'mcx>, bool)` (fmgr-seams) and installed `function_call_invoke_datum_seam`
   (fmgr-core init_seams) — a generalization of `function_call1_coll_datum_seam`
   to N args: each canonical arg crosses via `datum_to_ref_arg` (by-value word OR
   by-reference referent bytes), dispatched through `function_call_invoke_with_expr`,
   the result materialized back via `ref_out_to_datum`. Like `function_call_invoke_seam`
   it does NOT apply the `null_check` self-test (a function may legitimately return
   NULL via `fcinfo->isnull`, which the caller stores); the strict short-circuit is
   the interpreter's.

2. **Interp arg-gather routed through canonical Datums.** `func_step_inputs`
   (execExprInterp/eval_scalar.rs) now returns the per-arg canonical `Datum<'mcx>`
   (cloned straight from the result cell — the cell already carries the by-ref
   image) plus a parallel `isnull` vec, instead of flattening via `word_of`;
   `exec_func_step` takes `estate` (for `es_query_cxt` = the materialize mcx) and
   dispatches the new seam, writing the returned canonical `Datum<'mcx>` directly.
   The sibling `distinct_step_inputs` / `exec_distinct_step` / `exec_nullif_step`
   (EEOP_DISTINCT / NOT_DISTINCT / NULLIF — they read the cell and feed real fmgr
   calls) were widened the same way in-lane (clean: the make_ro transform already
   spoke canonical `Datum`).

`exec_rowcompare_step` and `iocoerce_core` were left on the bare-word lane (their
`fcinfo_data.args: Vec<NullableDatum>` payload-struct frame would need widening) —
documented fast-follow, off the milestone path.

Verified live (multi-process postmaster): `WHERE relname = 'pg_type'` → 1 row,
`… = 'pg_type' OR … = 'pg_class'` → 2 rows (the OR-seqscan-filter case),
`… AND relnamespace = 11` → 1 row, `SELECT 1+1` → 2, `SELECT relname FROM
pg_class` → 415 rows, second connection `SELECT 42` → 42. No regression.

## WALL 1ar — current furthest point (planner scalar-inequality selectivity by-ref const)

`SELECT relname FROM pg_class WHERE relname > 'pg_a'` (a text/name range qual)
now crashes EARLIER — in the **planner's selectivity estimator**, not the
executor. `scalarineqsel_wrapper` (selfuncs `entry.rs:153`) flattens the by-ref
`Const.constvalue` via `c.constvalue.as_usize()` before handing `constval` to
`scalarineqsel`, panicking `Datum: scalar accessor called on a by-reference value`
(`backend_access_common_heaptuple.rs:139`). Frame: `entry.rs:153
(scalarineqsel_wrapper) ← dispatch.rs:82 ← plancat.rs:1357 (clause selectivity) ←
allpaths/planner`. This is precisely the WALL 1ai documented follow-on ("the
scalar-inequality / mcv_selectivity / histogram legs keep the bare-word carrier —
same future keystone"): a planner-side by-ref const-carrier widening (defer the
`as_usize()` extraction to the actual MCV/histogram comparison, as 1ai did for
`var_eq_const`), NOT an executor/execExpr issue. Out of the 1aq lane. (Secondary,
off-path: `EXPLAIN` unported — analyze.c transformStmt T_ExplainStmt panic;
`= ANY(literal array)` walls earlier in planner const-fold at clauses-seams:98 /
fold.rs:1528.)

## WALL 1h — CLEARED

`get_subscription_list` (launcher.c) was unported; the logical-replication
launcher child aborted on it, triggering postmaster crash-recovery so
`AcceptConnection` never stabilized.

Fixed: ported `get_subscription_list` faithfully into
`backend-catalog-pg-subscription` (the catalog/heapam/xact read owner) over the
existing `StartTransactionCommand` → `table_open(SubscriptionRelationId,
AccessShareLock)` → keyless `systable_beginscan`/`systable_getnext`
(`table_beginscan_catalog`/`heap_getnext` analog) → `heap_deform_tuple` →
`CommitTransactionCommand` machinery. Decodes the launcher-relevant columns
(`oid`=attnum 1, `subdbid`=2, `subowner`=5, `subenabled`=6, `subname`=4,
cross-checked against `pg_subscription_d.h`) into the trimmed
`types_replication_launcher::Subscription` carrier the launcher consumes. On a
fresh DB it returns the empty list, so the launcher's worker-spawn loop is
skipped and it proceeds to snapshot acquisition. Seam installed in
`init_seams`; allowlist entry removed from `seams-init`. (The sibling
`check_subscription_relkind` seam in the same DESIGN_DEBT comment was left
allowlisted — its faithful owner is `executor/execReplication.c`, NOT
`pg_subscription.c`, and execReplication is unported.)

The postmaster now reaches **"database system is ready to accept connections"**
(PM_RUN), forks the launcher, and the launcher executes the real
`get_subscription_list` catalog scan.

## WALL 1i — CLEARED

The launcher's first catalog snapshot aborted on an uninitialized
`ShmemVariableCache->oldestXid` (procarray `FullXidRelativeTo` debug_assert).
Root cause was NOT missing seeding logic: `StartupXLOG` faithfully seeds
`nextXid`/`oldestXid`/`oldestXidDB`/`latestCompletedXid` + the derived limits
via the real (ported, installed) `SetTransactionIdLimit` / `MultiXactSetNextMXact`
/ `SetMultiXactIdLimit` / `AdvanceOldestClogXid` seams, in C order
(xlog.c:5634-5642, 6144-6148).

The bug is the fork/COW process model. The genuinely-shared C structs
(`TransamVariables`, `MultiXactState`) are modelled as process-local statics
(`TRANSAM_VARIABLES` = `static Mutex<…>`, varsup lib.rs:67; multixact
`with_state`). "Shared" means: the postmaster populates them once via
`CreateSharedMemoryAndSemaphores` before forking, and children inherit by
`fork()` copy-on-write. But `StartupXLOG` runs in the **startup child**
(backend-postmaster-startup:295) which `proc_exit`s — its seeding lands only in
that child's private COW copy and dies with it. The postmaster's copy stays
zeroed, so the launcher / backends it later forks inherit `oldestXid == 0` and
trip the snapshot horizon assert. (Single-user is unaffected: one process runs
StartupXLOG and then the same query — its private copy IS the only copy.)

Fix (COW-model faithful, mirrors what real shared memory does): added
`SeedTransamVariablesFromCheckpoint()` (xlog `startup.rs`) — the exact same
seam-call sequence StartupXLOG uses, reading `ControlFile->checkPointCopy` —
exposed as the `seed_transam_variables_from_checkpoint` xlog seam (installed in
xlog `init_seams`), and called by the postmaster reaper at the "startup process
succeeded → PM_RUN" transition (reaper.rs, before forking launcher/backends).
This re-seeds the postmaster's COW copy from the control file it already holds
(via `LocalProcessControlFile`), so every later child inherits valid XID/multi
bounds. StartupXLOG itself is unchanged (single-user untouched).

Verified: multi-process postmaster reaches "ready to accept connections" with NO
launcher-crash / crash-recovery loop; it now forks a backend for the live psql
connection (previously the launcher aborted and crash-recovery cycled). The
forked backend then hits a NEW, distinct wall (1j, below). Single-user
`SELECT 1;` on a clean "shut down" template still returns `1` (result row
`1: ?column? = "1"`, typeid 23), then the pre-existing deferred `ShutdownXLOG`
shmem-exit panic — no regression.

## WALL 1j — CLEARED

`pg_set_noblock(sock)` (`src/port/noblock.c`) was unported — only the
`port-noblock-seams` crate existed and the forked client backend aborted on
`seam not installed: port_noblock_seams::pg_set_noblock` during connection
setup.

Fixed: ported `src/port/noblock.c` faithfully into a new owner crate
`crates/port-noblock/` (mirrors the sibling tiny `port-pgsleep`/`port-pqsignal`
structure). Implements the `#if !defined(WIN32)` unix path of `pg_set_noblock`
and `pg_set_block` over `libc::fcntl(F_GETFL)` / `fcntl(F_SETFL)` with
`O_NONBLOCK` (set / clear respectively), returning `true` on success — exact
mirror of the C control flow (the C source has no `EINTR` retry loop). The seam
crate declares only `pg_set_noblock`, so `init_seams()` installs that one;
`pg_set_block` is retained in the owner for completeness. Registered
`port_noblock::init_seams()` in `seams-init` (Cargo dep + `init_all` call). Both
seams-init recurrence guards pass.

Verified empirically: with the crate landed, the forked client backend no longer
panics on the noblock seam — it advances well past connection setup. WALL 1j is
cleared. The live connection still does not return a result row: the backend now
hits a NEW, distinct wall (1k, below) deeper in the connection / address path.

## WALL 1k — CLEARED

The live `psql SELECT 1` forked a client backend that aborted on a
misaligned-pointer dereference in `sockaddr_family()`
(`common-ip/src/lib.rs:301`): it cast the unaligned `SockAddr.addr` byte buffer
to `*const libc::sockaddr_storage` and dereferenced it (`&*…`) to read
`ss_family`, forming a misaligned reference (UB → Rust's
misaligned-pointer-dereference check aborts the backend, signal 6, triggering
postmaster crash-recovery).

Fixed (faithful, alignment-only; no behavior change): `crates/common-ip/src/lib.rs`.
- `sockaddr_family`: read `ss_family` via
  `ptr::addr_of!((*p).ss_family).read_unaligned()` — never forms a `&` to the
  misaligned location, and uses `addr_of!` of the named field so the
  platform-correct offset is honored (on macOS `ss_family` is at offset 1 after
  `ss_len`, NOT offset 0 — a naive offset-0 read would be wrong there).
- `getnameinfo_unix` (the other misaligned-ref site): replaced
  `&*(addr.addr.as_ptr().cast::<libc::sockaddr_un>())` with a
  `copy_nonoverlapping` of `salen` bytes into a properly-aligned local
  `sockaddr_un`, then read `sun_path` from that (what C effectively does, since
  its structs are aligned). Network byte order / field semantics unchanged.
- Audited the rest of the file: `getnameinfo_system` and `copy_addrinfo` only
  use raw-pointer casts for FFI (`cast::<sockaddr>()` handed to `getnameinfo`)
  or `copy_nonoverlapping`; neither forms a misaligned Rust reference. No
  `sin_port`/`sin_addr`/`sin6_addr`/`run_ifaddr_callback` code lives in this
  file. The two sites above were the only misaligned-reference patterns.
- Added a `common-ip` unit test (`sockaddr_family_reads_from_misaligned_buffer`)
  that builds a real aligned `sockaddr_in`, copies its bytes into the byte
  buffer, and asserts `sockaddr_family` reads the family without aborting (it
  would have panicked/aborted before the fix). `cargo test -p common-ip` = 10/10.

Verified empirically: with the fix, the multi-process postmaster reaches "ready
to accept connections", the live `psql -h <sock> -p 54400 -U postgres -d
postgres -c 'SELECT 1'` forks a client backend that gets **past**
`sockaddr_family` (ZERO `misaligned` panics anywhere in the postmaster log), and
crashes at a NEW, distinct point (WALL 1l, below). WALL 1k is cleared.

## WALL 1l — CLEARED

The live `psql SELECT 1` backend aborted during authentication on
`seam not installed: backend_postmaster_postmaster_seams::set_client_auth_in_progress`.
`PerformAuthentication` (backend-utils-init-postinit:132/:171) sets/clears the
`ClientAuthInProgress` flag (postmaster.c global), but its owner crate
`backend-postmaster-postmaster` never installed the seam.

Fix (owner = backend-postmaster-postmaster): installed `client_auth_in_progress`
+ `set_client_auth_in_progress` in `init_seams()`, wired to the EXISTING
canonical process-local flag `CLIENT_AUTH_IN_PROGRESS` thread-local in
`backend-utils-error::config` (already written by backend-tcop-backend-startup's
`BackendInitialize` at :217 and read by the error reporter to limit log
visibility during auth). Did NOT use the `core.rs:249 PostmasterState` field —
that would create a divergent second copy invisible to the tcop writer / error
reporter, and `pm()`/`pm_mut()` lazily init a whole `PostmasterState` per
process (the field is faithful only if everyone uses the same backing). Both
seams now delegate to the one per-process flag. No allowlist entry existed.

## WALL 1m — CLEARED

Backend then aborted on `hba_getauthmethod: MyProcPort is NULL`
(backend-libpq-hba/src/loaders.rs:331). Root cause was a re-entrancy bug in the
seam wiring, not a C divergence: `client_authentication_entry` reads the
ambient `MyProcPort` via `with_my_proc_port`, whose owner (`WithMyProcPort`,
init-small globals.rs:539) does `MY_PROC_PORT.take()` for the closure duration
(so re-entrant reads observe it unset, by design). Inside that closure,
`ClientAuthentication(port)` calls `hba_getauthmethod()`, whose old seam took
NO arg and re-read the now-taken `MyProcPort` → `None` → panic. In C,
`hba_getauthmethod(hbaPort *port)` takes the port as an argument, threaded
straight down from `ClientAuthentication` (auth.c:390).

Fix: re-signed the `hba_getauthmethod` seam to take `port: &mut types_net::Port`
(matching C), pass the caller's live `port` from `ClientAuthentication`
(auth lib.rs:429), and dropped the ambient re-read in the owner
(`hba_getauthmethod_entry` now just calls `hba_getauthmethod(port)`). Added
`types-net` dep to `backend-libpq-auth-seams`.

## WALL 1n — CLEARED

Backend then aborted on a misaligned-pointer dereference at
`backend-libpq-hba/src/matchers.rs:44` (signal 6 → crash-recovery). Same class
as WALL 1k: `SockAddr.addr` is a raw unaligned `[u8; 128]`; `ss_family`,
`sockaddr_to_ipaddr`, and `ipaddr_to_sockaddr` cast it to
`*const/*mut sockaddr_storage`/`sockaddr_in`/`sockaddr_in6` and formed `&`/`&mut`
references → UB → Rust's misaligned-deref check aborts.

Fix (alignment-only, no behavior change): `ss_family` reads via
`addr_of!((*p).ss_family).read_unaligned()` (mirrors the common-ip WALL 1k fix,
honoring the platform `ss_family` offset). The two IPv4/IPv6 reads
`copy_nonoverlapping` the unaligned bytes into an aligned local `sockaddr_in`/
`sockaddr_in6` before reading `sin_addr`/`sin6_addr`. `ipaddr_to_sockaddr`
fills an aligned local then copies its bytes into the storage buffer. Audited
the rest of the crate — no other raw sockaddr derefs (the mask arithmetic in
`backend-libpq-ifaddr` goes through `IpAddr`).

## WALL 1o — CLEARED

Backend aborted on `seam not installed:
backend_libpq_auth_seams::log_connection_authentication`. auth.c reads
`log_connections & LOG_CONNECTION_AUTHENTICATION` to decide whether to log the
per-method "connection authenticated" line. The `log_connections` aspect-flag
mask is owned by `backend-tcop-backend-startup` (backend_startup.c,
check/assign_log_connections). Fix: installed the seam there, reading
`log_connections::get() & LOG_CONNECTION_AUTHENTICATION != 0`; added
`backend-libpq-auth-seams` dep.

## WALL 1p — CLEARED

Backend aborted on `seam not installed:
backend_libpq_auth_seams::client_authentication_hook`. This is the
`ClientAuthentication_hook` auth.c global function pointer — the optional
auth-extension plugin point, NULL unless a loadable module assigns it; the C
call site is guarded `if (ClientAuthentication_hook) (*hook)(port, status)`.
No such module is loaded in this build, so the hook is NULL and the call is a
no-op. The global lives in auth.c, so installed the seam in
`backend-libpq-auth::init_seams()` to the NULL-hook behavior (do nothing,
return Ok).

## WALL 1q — CLEARED

Backend then reached InitPostgres and reported a clean
`FATAL: role with OID 0 does not exist` (no abort, no crash-recovery). Root
cause: `InitializeSessionUserId` was called with `rolename = None`, `roleid = 0`
because `PostgresMain` (main_loop.rs) discarded its `username` argument
(`let _username = username; // role resolved internally`) and called the
slotsync-flavored `init_postgres(dbname)` seam, which hardcodes a NULL username.
In C, `PostgresMain(dbname, username)` (postgres.c:4289) passes
`username` (= `MyProcPort->user_name`, threaded from `BackendMain`,
backend_startup.c:124) into
`InitPostgres(dbname, InvalidOid, username, InvalidOid,
(!am_walsender) ? INIT_PG_LOAD_SESSION_LIBS : 0, NULL)`.

Fix: in `postgres_main_inner`, thread the (non-NULL, asserted) `username`
through to `init_postgres_by_name(Some(dbname), Some(username), init_flags)`
with `init_flags = am_walsender ? 0 : INIT_PG_LOAD_SESSION_LIBS` — the
full-signature InitPostgres seam that already exists for the background-worker
path. Single-user (single_user.rs:157) also passes a real username, so it now
correctly resolves the role by name too; `SELECT 1;` still returns the row
(verified, then the pre-existing deferred ShutdownXLOG shmem panic — no
regression).

## WALL 1r — CLEARED

The bgwriter child aborted on `seam not installed:
backend_access_transam_xlog_seams::log_standby_snapshot`
(bgwriter main_loop_cycle -> BackgroundWriterMain), triggering crash-recovery
which closed the live connection. There are two log_standby_snapshot seams: the
mcx-taking owner `backend_storage_ipc_standby_seams::log_standby_snapshot(mcx)`
was installed, but the public no-`mcx` forwarding variant
`backend_access_transam_xlog_seams::log_standby_snapshot()` consumed by
bgwriter/xlogfuncs/snapbuild/slot was never installed.

Fix (owner = backend-storage-ipc-standby): added a no-`mcx` forwarding wrapper
`log_standby_snapshot_seam` and installed it. The mcx crate deliberately has no
ambient current context (design: thread `Mcx` through parameters), so — exactly
as the sibling `standby_redo_seam` does — the wrapper creates a private
throwaway `MemoryContext`, derives `ctx.mcx()`, and forwards into
`LogStandbySnapshot(mcx)`. In C `LogStandbySnapshot(void)` runs in
CurrentMemoryContext and palloc/pfrees its transient GetRunningTransactionLocks
array there; the throwaway context is freed when the wrapper returns, mirroring
that. (No CurrentMemoryContext->Mcx bridge primitive was needed — the prior
lane's "mcx-bridge keystone" framing was wrong; the established throwaway-context
pattern is the faithful answer.) Commit f620fff58.

## WALL 1s — CLEARED

The bgwriter then aborted twice deeper on the running-xacts logging path.
(a) `seam not installed:
backend_storage_lmgr_lock_seams::get_running_transaction_locks`
(LogStandbySnapshot -> it). Faithfully ported `GetRunningTransactionLocks`
(lock.c:4141) into the lock owner: acquire all NUM_LOCK_PARTITIONS partition
LWLocks in order, scan the PROCLOCK table (the (LOCKTAG,ProcNumber)-keyed
state::SHARED.proclocks map), collect every proclock holding AccessExclusiveLock
on a LOCKTAG_RELATION whose holder PGPROC has a valid xid (proc_xid seam; skip
zeroed), release in reverse order, return PgVec<xl_standby_lock> in the caller's
`mcx`. Installed the seam.
(b) `debug_assert!(TransactionIdIsValid(running.nextXid))` in
GetRunningTransactionData (procarray snapshot.rs:611). Same COW-model class as
WALL 1i: TransamVariables is a process-local static "shared" by fork() COW. The
bgwriter/checkpointer are forked early (pmState == PM_STARTUP, before the startup
process finishes StartupXLOG), so they inherit an unseeded postmaster copy
(nextXid == 0). The WALL 1i reseed runs in the postmaster at PM_RUN but can't
reach an already-forked child. Fix: the bgwriter re-seeds its own copy via the
existing `seed_transam_variables_from_checkpoint` seam right after
auxiliary_process_main_common, reading the control file the postmaster loaded
(LocalProcessControlFile in PostmasterMain) and the bgwriter inherits by COW.
Commit 29148159f.

## WALL 1t — CLEARED

The bgwriter then inserted the xl_running_xacts WAL record and aborted in
CopyXLogRecordToWAL on `debug_assert_eq!(written, write_len)` (insert.rs:675),
via LogStandbySnapshot -> GetRunningTransactionData -> LogCurrentRunningXacts ->
XLogInsertRecord -> CopyXLogRecordToWAL.

Root cause: `XLogRecordAssemble` *consumed* the registered data chain via
`core::mem::take(&mut state.mainrdata)` (and per-buffer
`registered_buffers[i].rdata`) when moving chunks into the body span list, but
left `mainrdata_len` intact. `XLogInsert` wraps assemble + insert in a retry
loop; when `XLogInsertRecord` returns InvalidXLogRecPtr (the full-page-writes
restart — fires on the first post-boot record because prev_do_page_writes is
false while fullPageWrites is on), it re-assembles. On the second pass mainrdata
was already drained, so the body carried no main-data span yet xl_tot_len still
counted it. Empirically `fraglens=[26] write_len=50` (the 24-byte running-xacts
body dropped). In C, XLogRecordAssemble never destroys the registered chain (it
builds a separate hdr_rdt and links to the intact chain, cleared only by
XLogResetInsertion after success). Fix: clone the chunks into the body instead
of taking them, leaving the registered chain intact for retries. Commit
(cherry-picked) on this branch.

## WALL 1u — CLEARED

NOTE: the documented bgwriter `st_changecount` assert did NOT reproduce at base
`8c6c1c932` (it must have been resolved by a commit folded into the base, or the
walwriter aborts first). The actual furthest point at base was the **walwriter**
child aborting on a GUC slot:

```
thread 'main' panicked at crates/backend-utils-misc-guc-tables/src/slots.rs:85:
GUC slot enableFsync used before its owning unit installed it
```

Backtrace: WalWriterMain -> main_loop_cycle -> XLogBackgroundFlush -> XLogWrite
-> XLogFileInit -> XLogFileInitInternal -> get_sync_bit -> enable_fsync ->
`vars::enableFsync.read()`. The `fsync` GUC's `conf->variable` is the globals.c
`bool enableFsync` (init-small globals.rs already has the ENABLE_FSYNC cell +
enableFsync/set_enableFsync accessors), but init-small's `init_seams` never
installed the `vars::enableFsync` GucVarAccessors slot.

Fix (owner = globals.c = backend-utils-init-small): install
`vars::enableFsync.install(GucVarAccessors { get: globals::enableFsync,
set: globals::set_enableFsync })` alongside the other globals.c-backed GUC
accessors in `init_seams`. No new backing store. Commit fb08d2355.

## WALL 1v — CLEARED

The postmaster's ServerLoop then aborted on `seam not installed:
backend_postmaster_postmaster_seams::recheck_data_dir_lock_file`
(serverloop.rs:255 -> the periodic postmaster.pid recheck, postmaster.c:1781).
`RecheckDataDirLockFile` (miscinit.c:1697) is already fully ported in
backend-utils-init-miscinit (lockfile.rs:537, `PgResult<bool>`) but the seam —
declared on backend-postmaster-postmaster-seams and consumed by the postmaster —
was installed by nobody.

Fix (owner = miscinit.c): add a dep on the thin
backend-postmaster-postmaster-seams crate (no cycle — it only deps
seam-core/types-*) and install the seam from miscinit's `init_seams`, delegating
to `RecheckDataDirLockFile().unwrap_or(true)`. C returns bool and never longjmps
(only LOG ereports); per the C contract ("return true if there is any doubt: we
do not want to cause a panic shutdown unnecessarily"), an unexpected Err maps to
true. Commit 93749d2cb.

## WALL 1w — CLEARED

The proc freelist COW keystone is fixed: the four `ProcGlobal` freelist heads
(`freeProcs`/`autovacFreeProcs`/`bgworkerFreeProcs`/`walsenderFreeProcs`) and the
per-PGPROC `links` that thread them now live in a genuine shared-memory segment
(mirroring the already-shared pid words + `ProcStructLock`), so every forked
backend pops a **distinct** ProcNumber instead of all popping ProcNumber 0 and
colliding on the genuinely-shared sinval slot array.

Fix (owner = proc.c = `backend-storage-lmgr-proc`, `proc_shmem.rs`): added a
shared `[FreeLink; total_procs]` (next/prev ProcNumber, -1 == detached — the
realization of `PGPROC.links`) and a shared `[ListHead; 4]` (head/tail
ProcNumber, -1 == empty — the four `dlist_head`s), both placed via
`ShmemInitStruct` in `init_shared_freelists` (called from `InitProcGlobal`
right after `init_shared_pid_block`). `freelist_pop_head`/`push_head`/`push_tail`
now operate on this shared intrusive dlist under `ProcStructLock` (every caller
already holds the spinlock bracket). `InitProcGlobal`'s initial freelist
threading uses the same shared push (no lock — postmaster, pre-fork, like C).
The process-local `PROC_HDR.{freeProcs,...}` fields are left in place but
unused (`procgloballist` class still COW-inherited read-mostly). Faithful to C
`InitProcGlobal`, which `ShmemInitStruct`s the PGPROC block (with `links`) and
threads the dlists in shared memory. Verified empirically: the live
`psql SELECT 1` no longer gets the `sinval slot for backend 0 is already in
use` error — the forked backend advances past `SharedInvalBackendInit` into
InitPostgres. Commit on branch `postmaster-wall-1w`.

## WALL 1x — CLEARED (GUC store re-entrancy deadlock)

With 1w past, the forked backend then **deadlocked** (not panicked) in
InitPostgres -> InitializeSessionUserId -> SetSessionAuthorization ->
SetOuterUserId, re-entering the GUC machinery. Backtrace: `set_config_option_global`
holds the `GUC_STORE` `Mutex` via `with_store_mut` (frame ~6), then the value's
`assign_session_authorization` hook fires inline inside `apply_value`, runs
`SetSessionAuthorization` -> `SetOuterUserId` -> `SetConfigOption("is_superuser")`
-> `set_config_option_global` -> `with_store_mut` again -> **re-lock of the
non-reentrant Mutex -> deadlock**. In C this works because the GUC store is plain
file-static memory (no lock) and the recursion is strictly nested; the Rust
port's `Mutex` (added for the multi-threaded test harness) cannot be re-entered,
and the outer `&mut reg` borrow would alias.

Fix (owner = guc.c = `backend-utils-misc-guc`): the variable's `assign_hook` is
no longer fired inline by `apply_value` (registry.rs). `apply_value` now sets the
value + writes owner storage (neither re-enters the GUC store) and **returns the
captured assign-hook invocation** (`DeferredAssignHook = Box<dyn FnOnce()>`);
`set_config_option` collects them into a caller-supplied `&mut Vec`; the lock
holders (`set_config_option_global` in live.rs, and the `RestoreGUCState` path in
lib.rs) **fire the deferred hooks AFTER releasing the store borrow**. So a
recursively re-entrant `SetConfigOption` no longer re-locks the store / aliases
the live `&mut reg`, exactly matching C's synchronous-after-value-set hook
ordering. `serialize::restore_guc_state` + the test caller were threaded the same
`&mut Vec`. Verified: the deadlock is gone; the backend advances to WALL 1y.
Commit on branch.

## WALL 1y — CLEARED (pgstat_report_appname seam install)

The backend then aborted on `pgstat_report_appname (backend_status.c) not yet
ported` — the panic-stub install for the `application_name` GUC's
`assign_application_name` assign hook (commands/variable.c, installed at
`backend-commands-variable/src/lib.rs:1369`). The real `pgstat_report_appname`
was **already fully ported** in its owner (`backend-utils-activity-status`,
backend_status.c, lib.rs:875) — it just was never installed into the consumer's
seam crate (`backend-commands-variable-seams`).

Fix (owner = backend_status.c = `backend-utils-activity-status`): cross-installed
`backend_commands_variable_seams::pgstat_report_appname` from the status crate's
`init_seams` (delegating to the existing `pgstat_report_appname(&[u8])`), exactly
like the existing `backend_postmaster_postmaster_seams` cross-install in the same
function; added the `backend-commands-variable-seams` dep (no cycle — that seam
crate only deps seam-core/mcx/types-*). Removed the panic-stub install in
variable.c. Verified: the appname panic is gone; the backend advances to the new
current wall (1z).

## WALL 1z — CLEARED (relcache init-file rd_amhandler not restored)

The `amhandler == 0` panic was NOT a syscache-decode bug. `search_am_handler`
(the AMOID projection) returns the correct handler (verified live: for amoid=403
it yields amhandler=330 every call). The 0 reached `GetIndexAmRoutine` from the
OTHER caller: `InitIndexAmRoutine` (relcache index.rs:241) reads
`rd.rd_amhandler`, and on the **relcache init-file load path**
(`initfile.rs:1178`, `load_relcache_init_file`) that field was never restored.

Root cause: in C, `load_relcache_init_file` reads the WHOLE `RelationData`
struct with one `fread(rel, sizeof(RelationData))` (relcache.c:6235), so the
top-level `rd_amhandler` field comes back for free before `InitIndexAmRoutine`
(relcache.c:6348) calls the handler. This port reconstructs the entry
field-by-field and decoded `rd_rel` (incl. `relam`) but never set
`rd_amhandler`, so it was 0 (default) when `InitIndexAmRoutine` fired.

Fix (owner = relcache init-file loader): before `InitIndexAmRoutine` in the
index branch of `load_relcache_init_file`, resolve
`rel.rd_amhandler = search_am_handler(rel.rd_rel.relam)` via the AMOID syscache —
exactly as `RelationInitIndexAccessInfo` does for the from-catalog path (pg_am is
always heap-scannable; `AMOID`/`AMNAME` force heap scans). With this, every
nailed/critical index loaded from `pg_internal.init` gets its handler (btree→330)
and the built-in AM dispatch resolves. The AM-handler panic is gone.

## WALL 1aa — CLEARED (duplicate `whereToSendOutput`: forked backend took interactive path)

With 1z past, the forked client backend reached the main command loop but
`ReadCommand` ran `InteractiveBackend` (printed `backend> `, read stdin) instead
of `SocketBackend`, so it never spoke libpq — psql saw "server closed the
connection". Worse, the desync led the backend to SIGILL (signal 4) on the
second/odd path, triggering postmaster crash-recovery.

Root cause = a divergent duplicate of the single C global
`CommandDest whereToSendOutput` (postgres.c:91, default `DestDebug`). The port
had TWO cells: `backend-utils-error::config::WHERE_TO_SEND_OUTPUT` (default
`None`) — read by the error reporter and **written to `DestRemote` by
`BackendInitialize`** (backend_startup.c:180 mirror, backend-startup lib.rs:248)
— and a SECOND `backend-tcop-postgres::globals::WHERE_TO_SEND_OUTPUT` (default
`Debug`) read by `ReadCommand` (main_loop.rs:314) and exposed via the
`backend_tcop_postgres_seams::where_to_send_output` seam to postmaster / async /
walsender / syncrep. `BackendInitialize`'s `DestRemote` landed only in the
error-config copy, so `ReadCommand` still saw `Debug` → interactive path. Same
class as WALL 1l (`ClientAuthInProgress` had two copies).

Fix (collapse to ONE canonical cell, mirroring WALL 1l): made
`backend-utils-error::config::WHERE_TO_SEND_OUTPUT` the single home (default
corrected `None`→`Debug` = C postgres.c:91) and made tcop-postgres
`globals::where_to_send_output`/`set_where_to_send_output` delegate to it
(tcop-postgres already deps backend-utils-error; no cycle). Now `BackendInitialize`'s
`DestRemote`, the error reporter, `ReadCommand`, and the seam all read/write the
same variable. Single-user keeps `Debug` (the C default, unchanged → interactive,
correct).

Verified: a fresh multi-process boot + `psql -h <sock> -p 54400 -U postgres -d
postgres -c 'SELECT 1'` now returns the row `1` (exit 0) end-to-end over the
socket, server stays up (no crash-recovery on the first connection). Single-user
`SELECT 1` still returns `?column? = "1"` (typeid 23) then the pre-existing
deferred ShutdownXLOG panic — no regression. **This is the headline live
multi-process `psql SELECT 1` milestone.**

## WALL 1ab — CLEARED (init-file load path: amhandler re-derivation recursed before pg_class existed)

The SIGILL was NOT a shared-memory race. It was an **infinite-recursion stack
overflow** (the SIGILL/`ESR_EC_DABORT_EL0` is the blown-stack tail; the forked
backend's core is ~6 GB). The first backend writes `pg_internal.init`; the
**second** backend therefore takes the init-file LOAD path (the first takes
formrdesc), which is the 1st-works/2nd-fails asymmetry — same shape as the WALL 1z
init-file lineage.

Root cause (lldb on the 2nd-backend core): in
`RelationCacheInitializePhase2 → load_relcache_init_file(shared=true)` the WALL 1z
fix re-derived a nailed INDEX entry's `rd_amhandler` via
`search_am_handler → SearchSysCache1(AMOID)`. But the SHARED Phase2 load runs
**before** pg_class is built (Phase3), so the AMOID syscache scan opens pg_class →
`ScanPgRelation(pg_class) → table_open(pg_class) → RelationBuildDesc(pg_class) →
ScanPgRelation(pg_class) → …` recurses forever. The first backend never reached
that line (load returned false, no file). In C `load_relcache_init_file` does a
whole-`RelationData` `fread` that restores `rd_amhandler` for free — never a
syscache lookup.

Fix (owner = relcache init-file codec, `initfile.rs`): **persist `rd_amhandler`
in the init file** — `write_entry` frames it in the per-entry header, the load
header decode reads it back, and the index branch uses the restored value instead
of the recursing syscache call (a pre-`rd_amhandler` file is treated as corrupt →
`read_failed` → rebuild from catalog). This mirrors C's struct-image restore.
Also installed `backend_postmaster_postmaster_seams::local_process_control_file`
from the xlog owner (delegating to `shmem::LocalProcessControlFile`) so the
crash-restart reaper no longer secondarily panics.

Clearing 1ab exposed three more init-file-load / rebuild bugs of the same class,
all fixed in this lane (the second connection now returns the row, server stays
up across 10+ sequential and concurrent connections):

- **WALL 1ac (md):** `RelationCloseSmgr` is C's `if (rd_smgr != NULL) smgrclose`,
  a no-op when the relation's smgr was never opened. The port had no `rd_smgr`
  field and called `smgrclose` unconditionally → `md_close` → `md_run` panicked
  on an absent `MdRelnState`. Fix (`smgr-smgr` `relation_close_smgr` install):
  guard on `md::cache_contains(key)` (the `rd_smgr != NULL` analog).
- **WALL 1ad (rd_supportinfo):** the init-file load left `rd_supportinfo` empty;
  `index_getprocinfo` then indexed out of bounds. Fix (`initfile.rs` index
  branch): size it `natts * amsupport` zero-filled `FmgrInfo`, exactly like the
  from-catalog `RelationInitIndexAccessInfo`.
- **WALL 1ae (RefCell double-borrow):** `RelationReloadNailed` ran under a held
  `with_rel_mut(pg_class)` borrow, but its `ScanPgRelation` re-opens pg_class and
  re-borrows the same cell → `RefCell already mutably borrowed`. C marks
  `rd_isvalid=true` before the scan to break recursion but holds no exclusive
  borrow. Fix: re-sign `RelationReloadNailed(relation: Oid)` and use short scoped
  borrows around the un-borrowed catalog scan.
- **WALL 1af (refcnt on rebuild swap):** `RelationRebuildRelation`'s wholesale
  `mem::replace` carried the old entry's nonzero `rd_refcnt` onto the discarded
  temporary, tripping `RelationDestroyRelation`'s `HasReferenceCountZero` assert.
  C's `SWAPFIELD(rd_refcnt)` leaves 0 on the discarded entry. Fix: reset
  `newrel.rd_refcnt = 0` before destroy.

Verified: a fresh multi-process boot + 10 sequential `psql SELECT 1` (all return
`1`, exit 0) + 2 concurrent + a final liveness check — zero crash-recovery, zero
panics. Single-user `SELECT 1` still returns `?column? = "1"` (typeid 23) then the
pre-existing deferred ShutdownXLOG panic — no regression.

## WALL 1ah — CLEARED (by-reference Const carrier, bounded #113 slice)

The by-reference `Const` keystone is fixed as the architect's BOUNDED slice (not
the tree-wide model campaign the old framing claimed): the existing `Datum<'mcx>`
by-ref lane was extended into the type-input I/O seam + the `Const` carrier, and
the `'static`-erase reuses the established SubPlan/ParamListInfo long-lived-context
convention.

Two pinned `DatumWord` seam edges were widened to the canonical `Datum<'mcx>`:

1. **I/O seam.** `input_function_call` (fmgr-seams lib.rs:378) was re-signed from
   `DatumWord` to `types_tuple::…::Datum<'mcx>`; the owner
   (`input_function_call_seam`, fmgr-core) now classifies the `oid_input_function_call_out`
   `FmgrOut` (the by-ref-ready path that ALREADY existed) — `ByVal` → bare word,
   `Ref` → `ByRef` over the flattened payload in `mcx` — instead of collapsing
   through the panicking `fmgr_out_word`. Consumers updated: `stringTypeDatum`
   (parse-type) returns the canonical Datum; `domain_in` (misc2) threads it
   straight through (a domain over text now keeps its by-ref value);
   `range_in` (rangetypes) collapses to the bare element word at the
   `RangeBound.val` carrier edge (range bounds are a separate bare-word carrier —
   a by-ref range element is the rangetypes by-ref follow-on, unchanged from the
   prior DatumWord panic, off the milestone path).

2. **`Const` carrier.** `make_const` (makefuncs.c) and `coerce_unknown_const`
   (parse-coerce) no longer panic on `ByRef`: they `datumCopy` the (already-flat,
   detoasted) by-reference image into a leaked backend-lifetime `CONST_VALUE_CONTEXT`
   memory context — yielding a genuine `Datum<'static>` for the `Const.constvalue`
   field — mirroring `params::PARAM_LIST_CONTEXT` and the SubPlan `'static`-erase.
   This is exactly what C does (the input function's palloc'd varlena lives in the
   long-lived parse context the Const is built in).

Verified: a by-reference literal `Const` (e.g. `'pg_type'`) is now built and flows
end-to-end through parse → coerce → make_const → planner const-fold → selectivity.
Non-regressions hold: live multi-process `SELECT 1+1` → `2`,
`SELECT relname FROM pg_class` → 415 rows, a second connection `SELECT 42` → `42`.

NOTE: the headline `SELECT relname FROM pg_class WHERE relname = 'pg_type'` does
NOT yet return the row, but for a DIFFERENT, downstream reason — the planner picks
the unique-index path on `relname` and walls at the unported `create_indexscan_plan`
converter (WALL 1al below), not on any by-ref-Const issue. The 1ah keystone itself
is cleared; the by-ref value reaches createplan intact.

A secondary, independent wall (subquery-in-FROM only, not on the milestone path):
`SELECT … FROM (SELECT …) q` panics at
`backend-optimizer-prep-prepjointree/src/pullup.rs:2254: perform_pullup_replace_vars:
no jointree` — a planner subquery pull-up gap.

## WALL 1ai — CLEARED (eqsel by-reference const: deferred bare-word extraction)

After 1ah, the by-ref `Const` reached the selectivity estimator and panicked at
`scalar.rs` (`Datum: scalar accessor called on a by-reference value`) via
`eqsel_internal → var_eq_const`. `eqsel_internal` extracted the const value as a
bare word (`c.constvalue.as_usize()`) UNCONDITIONALLY before passing it to
`var_eq_const`, which only USES it inside the MCV-comparison loop (and on a fresh
cluster the MCV slot is empty → the loop never runs). C passes the `Datum`
(a pointer for by-ref) cheaply and only dereferences it at the comparison.

Fix (faithful, bounded): re-signed `var_eq_const` to take the canonical
`Datum<'mcx>` (by ref); `eqsel_internal`/`node_sel` thread `&c.constvalue`; the
bare-word/pointer extraction is DEFERRED to the MCV loop body (computed per-actual-
comparison), so a by-ref const on the no-MCV path never needs the word. A by-ref
const compared against ACTUAL MCV slot values (only with ANALYZE'd stats) is the
selfuncs by-reference value-carrier follow-on (the MCV slot values are themselves
bare pointer words from the C-shaped `pg_statistic` tuple) and is precisely
documented as that keystone — unreachable on the fresh-cluster milestone path.
The scalar-inequality / mcv_selectivity / histogram legs keep the bare-word carrier
(same future keystone). Added `types-tuple` dep to selfuncs.

## WALL 1aj — CLEARED (is_redundant_with_indexclauses cross-install)

Next, `cost_index → extract_nonindex_conditions` aborted on `seam not installed:
…costsize_seams::is_redundant_with_indexclauses`. The real impl lives in
equivclass.c (`relevance.rs`, taking `&[IndexClause]`) but the public seam is
declared on costsize-seams (carrying the index path by `PathId`) and was installed
by nobody. Fix (owner = equivclass): cross-installed the seam from equivclass's
`init_seams`, resolving `PathId → IndexPath.indexclauses` before delegating to the
impl — exactly as relnode cross-installs `pathnode::relids_subset_compare`. Added
the thin costsize-seams dep (no cycle).

## WALL 1ak — CLEARED (pathnode-seams relids_* installs)

Then `choose_bitmap_and → create_bitmap_and_path` aborted on `seam not installed:
…pathnode_seams::relids_add_members`. The pathnode crate consumes a parallel set of
`relids_*` bitmapset wrappers declared on pathnode-seams (`relids_union`,
`relids_add_members`, `relids_del_members`, `relids_equal`), of which only
`relids_subset_compare` was installed. Fix (owner = relnode, the bitmapset-algebra
owner): installed the four from the existing `bms_*` impls alongside the existing
`pathnode::relids_subset_compare` install (`del_members(a,b)` = `bms_difference(&a,b)`).

## WALL 1al — current furthest point (create_indexscan_plan converter unported)

With 1ah–1ak cleared, the by-ref `Const` flows through planning and the planner
chooses the unique-index path on `relname` for `WHERE relname = 'pg_type'`; it then
aborts at `seam not installed: …createplan_seams::create_indexscan_plan`
(`create_scan_plan → create_plan_recurse`). This seam is GENUINELY UNPORTED — the
createplan crate calls it via seam but no body exists anywhere. `create_indexscan_plan`
(createplan.c:2989, ~300 LOC) + its `fix_indexqual_references` / `fix_indexqual_clause`
/ `fix_indexqual_operand` index-qual-rewriting machinery is a full converter port,
a distinct lane (NOT a seam install or carrier fix). The `enable_indexscan=off`
GUC only adds a cost penalty, so the unique-index equality path still wins; the
headline query cannot return the row until this converter lands. Next lane = WALL
1al: port `create_indexscan_plan` (the `backend-optimizer-util-vars/fix_indexqual.rs`
substrate already exists as groundwork) and install the createplan seam.

## WALL 1ag — CLEARED (planner const-fold `fmgr_call` seam installed)

A live `SELECT 1+1` / `SELECT relname FROM pg_class` crash-recovered the server on
the **uninstalled `backend_optimizer_util_clauses_seams::fmgr_call` seam**
(clauses-seams lib.rs:79) — the `evaluate_expr` / const-fold fmgr leg reached
during planning (the same keystone noted for `LIMIT n`).

FIX: installed `fmgr_call` faithfully from its real owner, the fmgr dispatch
(`backend-utils-fmgr-core`). The new `fmgr_call_seam` (fmgr-core lib.rs) runs
`fmgr_info(funcid)` + a direct `FunctionCallInvoke`-shaped dispatch over the
constant argument values: it applies the executor's `EEOP_FUNCEXPR_STRICT`
short-circuit (a strict function with any NULL arg folds to NULL without calling),
threads each arg's NULL flag and by-reference referent through the fmgr boundary
side channel (`datum_to_ref_arg`), dispatches via
`function_call_invoke_with_expr` (NOT `invoke_flinfo`, so a non-strict NULL result
is read back from `fcinfo->isnull` rather than tripping the `function returned
NULL` self-test), and materializes the result into the caller's `mcx`
(`ref_out_to_datum`). `makeConst`'s detoast/`datumCopy` tail stays on the clauses
side. The seam was re-signed from the unsound `Datum<'static>` (no-mcx) contract
to the established by-reference Datum lane `<'mcx>(mcx, …, Vec<(Datum<'mcx>, bool,
Oid)>) -> (Datum<'mcx>, bool)` (matching `function_call1_coll_datum`), and its two
consumers (`fmgr_fold`, the NULLIF arm in clauses `fold.rs`) updated. Verified:
`SELECT 1+1` → `2`, `SELECT 5+7` → `12`, `SELECT 100/4` → `25`, `SELECT abs(-9)` →
`9`, `SELECT 3<5` → `t`; `SELECT relname FROM pg_class` returns all 415 rows;
multi-session `SELECT 1` on a second connection still returns a row.

## WALL 1z — original diagnosis (superseded by "WALL 1z — CLEARED" above)

With 1w/1x/1y cleared, the live `psql SELECT 1` forked backend now drives deep
into **InitPostgres / planning** and aborts (SIGABRT, non-unwinding panic, which
crashes the whole cluster via postmaster crash-recovery) at:

```
thread 'main' panicked at crates/backend-access-index-amapi/src/lib.rs:136:
index access method handler function 0 is not a built-in handler
(dynamic AM handler dispatch is not yet ported)
```

`GetIndexAmRoutine(amhandler=0)` was reached with a **zero handler OID**. The
caller is relcache `RelationInitIndexAccessInfo`
(`backend-utils-cache-relcache/src/index.rs:241`, `get_index_am_routine::call(
rd.rd_amhandler)`), where `rd_amhandler` was set at index.rs:291 from
`syscache::search_am_handler(rd.rd_rel.relam)` returning `Some(0)`. So the
`pg_am` row for a real built-in index AM (e.g. btree, whose `amhandler` should be
`bthandler` = OID 330 = `F_BTHANDLER`) is being read with `amhandler == 0`.

This is a genuine new wall (catalog/syscache decode, NOT a freelist/GUC issue):
the `AMOID` syscache projection `search_am_handler` (syscache projections.rs:246,
reading `Anum_pg_am_amhandler = 3` via `getattr_oid`) yields 0 for a built-in AM
on a fresh C-initdb cluster. Either the `pg_am` tuple's `amhandler` column is
being deformed to 0 (wrong attno / NULL handling / by-value decode bug in the
AMOID syscache load), or `rd_rel.relam` is itself resolving wrong. NB: the sister
path `GetIndexAmRoutineByAmId` *does* guard `amhandler == InvalidOid` and raises
a clean error; the relcache path at index.rs:241 calls `GetIndexAmRoutine`
directly, so the 0 reaches the built-in dispatch and panics. Next lane = WALL 1z:
trace which `relam`/index relation hits this and why the AMOID syscache yields
`amhandler == 0` for a built-in AM; fix the catalog/syscache decode (or the
relcache should route through the InvalidOid guard). This needs lldb/instrumented
inspection of the AMOID `SearchSysCache1` result for the offending index.

(The hundreds of `WARNING: resource was not closed` lines that flood the log on
the failing connection are pre-existing resource-owner leak warnings — they also
appear in the single-user `SELECT 1` run that DOES return the row — not the cause
of the crash; the SIGABRT above is.)

## WALL 1w — original diagnosis (superseded by "WALL 1w — CLEARED" above)

With 1u+1v cleared, the postmaster reaches "ready to accept connections" and the
live `psql SELECT 1` now connects far enough to get a **clean SQL error** back
(not a panic) from the forked client backend:

```
psql: ERROR:  sinval slot for backend 0 is already in use by process <pid>
```

(`SharedInvalBackendInit`, sinvaladt.c:296 -> sinval/src/lib.rs:657.)

Root cause = a genuine COW-shared-state keystone, NOT a leaf seam. The sinval
slot array lives in genuine byte-addressed shared memory (`ShmemInitStruct`), so
slot 0 really IS occupied cluster-wide. The bug is that **two regular backends
both claimed ProcNumber 0**. `InitProcess` (proc_lifecycle.rs:151) gets its
ProcNumber from `freelist_pop_head(Regular)` = the head of
`ProcGlobal->freeProcs`. But `PROC_GLOBAL` (the `PROC_HDR` holding all four
freelist heads) is a **`thread_local!`** (proc_shmem.rs:135) — process-local,
COW-inherited. Each forked backend inherits the postmaster's freelist with
proc 0 at the head, pops proc 0 (mutating only its private COW copy), and they
all collide on the genuinely-shared sinval slot array. The prior backend
registered proc 0 in shmem and its slot stays "in use".

Why this is a keystone (same class as WALL 1i/1s but harder): the proc freelist
is genuinely-shared **mutable** state that the COW model fundamentally cannot
replicate. Unlike the read-mostly XID bounds (reseed once at PM_RUN), the
freelist is mutated on every connect/disconnect, and the mutation must be
visible to the postmaster and all sibling backends. proc_shmem.rs already has
the genuine-shmem pattern for the **aux-process** case (the per-PGPROC `pid`
words + `ProcStructLock` live in a real shmem segment via `AtomicPtr` base
pointers, because `InitAuxiliaryProcess` scans pid words to find a free slot —
see proc_shmem.rs:143-160). But regular-backend slot assignment uses the
`freeProcs` linked-list head, which was left in the per-process `PROC_HDR`.

PREREQ (next lane / multi-session): move the four freelist heads
(`freeProcs`/`autovacFreeProcs`/`bgworkerFreeProcs`/`walsenderFreeProcs`) and the
per-PGPROC `links` that thread them into the genuine shmem segment (the same one
already holding the pid words + ProcStructLock), and make `freelist_pop_head`/
`freelist_push_head`/`freelist_push_tail` operate on shared memory under
ProcStructLock (they already run inside the spin_lock_acquire_proc_struct_lock
bracket in InitProcess/ProcKill). Then forked backends pop distinct ProcNumbers.
Faithful to C: `InitProcGlobal` ShmemInitStruct's the PGPROC block (with the
`links`/`procgloballist` fields) AND threads the four dlists in shared memory.
This is the proc-shmem freelist-sharing campaign — out of scope for a single
seam-install lane.

(Secondary, downstream and not blocking 1w: when one backend does proceed, the
checkpointer child later panics on `seam not installed:
backend_access_transam_xlog_seams::create_checkpoint` — a separate xlog seam
install, reachable once 1w is past.)

NOTE on the 1u/1v lane (branch `postmaster-wall-1u`, base `8c6c1c932`): worked
in a dedicated git worktree at `/tmp/pgrust-pm-1u` (NOT the main checkout, to
avoid the prior branch-switch contamination). Two wall commits (fb08d2355 1u,
93749d2cb 1v), each staging ONLY its own files (explicit `git add`, no `-A`).
Single-user `SELECT 1;` still returns `1` (typeid 23) then the pre-existing
deferred ShutdownXLOG shmem-exit panic — no regression. Guards (seams-init,
no-todo-guard) pass. Branch is NOT merged/pushed — left for the integrator.

NOTE on tree hygiene during the earlier 1r-1t lane: the shared working tree was
being concurrently edited by other agents (the branch was switched out from
under that lane mid-session, an empty
`crates/backend-utils-adt-pg-locale-icu-ffi` with no Cargo.toml repeatedly broke
`crates/*` glob workspace loading, and that lane's xloginsert edit was reverted
once and re-applied). Its three wall commits stage ONLY that lane's own source
files; WALL 1t was cherry-picked back onto agg-count-clone-in-fix after a foreign
branch switch. Also: `.cargo/config.toml`
on this branch no longer carries the `[env] PGRUST_PGSHAREDIR` from c3a8bcd4c, so
a clean `cargo build` must pass `PGRUST_PGSHAREDIR=/tmp/pgrust_share` in the
shell env (cargo does propagate it to `option_env!`); without it the binary
bakes the `/usr/local/pgsql/share` default and fails tz lookup at boot.

## WALL 1r — original diagnosis (superseded by "WALL 1r — CLEARED" above)

The live `psql SELECT 1` backend now resolves its role + database and aborts
deeper in InitPostgres / first-transaction on an uninstalled xlog seam:

```
thread 'main' panicked at crates/backend-access-transam-xlog-seams/src/lib.rs:327:1:
seam not installed: backend_access_transam_xlog_seams::log_standby_snapshot
```

IMPORTANT: this panic is in the **bgwriter child process**, NOT the psql
backend (backtrace: `backend-postmaster-bgwriter/src/lib.rs:316` ->
`main_loop_cycle` -> `BackgroundWriterMain`). The psql backend itself now
resolves role + database fine; the bgwriter aborts on this seam, triggering
postmaster crash-recovery which closes the live connection.

GOOD NEWS — the real impl ALREADY EXISTS and is installed: there are two
`log_standby_snapshot` seams:
 - `backend-storage-ipc-standby-seams::log_standby_snapshot(mcx) -> XLogRecPtr`
   — INSTALLED (standby/src/lib.rs:1464 -> the real `LogStandbySnapshot(mcx)`,
   standby/src/lib.rs:1213, fully ported).
 - `backend-access-transam-xlog-seams::log_standby_snapshot() -> XLogRecPtr`
   (NO mcx) — the public forwarding seam consumed by bgwriter/xlogfuncs/
   snapbuild/slot. This one is UNINSTALLED → the panic.

So WALL 1r is a **forwarding-seam install**, not a port. The natural owner is
`backend-storage-ipc-standby` (it already owns `LogStandbySnapshot` AND imports
`backend_access_transam_xlog_seams as xlog`, so it can install both): add
`xlog::log_standby_snapshot::set(...)` next to the existing
`seams::log_standby_snapshot::set(LogStandbySnapshot)` at :1464.

THE ONE BLOCKER to resolve first: the xlog-seams variant takes NO `mcx`, but
`LogStandbySnapshot(mcx)` needs one (it pallocs the transient
`GetRunningTransactionLocks` array in the caller's context). The forward must
materialize an `Mcx<'_>` from the current memory context
(`CurrentMemoryContext`) at the seam boundary. There is NO obvious
no-mcx -> mcx bridge helper in the `mcx` crate (checked: no `Mcx::current` /
`with_current_context` / root-scope entry), and constructing one unsoundly is a
lifetime hazard. Next lane = WALL 1r: find/define the faithful
`CurrentMemoryContext -> Mcx<'_>` scoped-entry bridge (the same primitive the
other no-mcx xlog-seams forwards to mcx-taking owners need), then install
`xlog::log_standby_snapshot` in `backend-storage-ipc-standby::init_seams()` to
forward into `LogStandbySnapshot`. This mcx-bridge is the real (small) keystone,
shared by any no-mcx public seam fronting an mcx-taking owner.

## WALL 1k — original diagnosis (superseded by "WALL 1k — CLEARED" above)

The live `psql SELECT 1` forks a client backend that now gets past `pg_set_noblock`
and aborts on a misaligned-pointer dereference:

```
thread 'main' panicked at crates/common-ip/src/lib.rs:301:23:
misaligned pointer dereference: address must be a multiple of 0x8 but is 0x16b1fe55c
```

`sockaddr_family()` (common-ip/src/lib.rs:301) casts a `&[u8]` byte buffer
(`SockAddr.addr`, an unaligned byte array) to `*const libc::sockaddr_storage` and
dereferences it to read `ss_family`. The byte buffer is not 8-byte aligned, so
the deref trips Rust's debug misalignment check and the client backend (PID
60485) aborts (signal 6), triggering postmaster crash-recovery — the connection
is closed before a result returns. This is a connection-address-handling
alignment bug in `common-ip`, NOT a noblock/varsup/xlog issue. Fix = read
`ss_family` via an unaligned read (`addr.as_ptr().add(offset_of!(ss_family))`
+ `read_unaligned`, or copy the bytes into an aligned `sockaddr_storage`) rather
than a direct aligned deref of the byte buffer. Next lane = WALL 1k.

## WALL 1i — original diagnosis (superseded by "WALL 1i — CLEARED" above)

The launcher's first catalog snapshot acquisition aborts. Exact chain (from the
backtrace, multi-process launcher child):

```
get_subscription_list (backend-catalog-pg-subscription/src/lib.rs)
  -> systable_beginscan (genam)
  -> GetTransactionSnapshot (backend-utils-time-snapmgr/src/lib.rs:244)
  -> GetSnapshotData / get_snapshot_data_into (procarray snapshot.rs:235)
  -> FullXidRelativeTo(latest_completed, oldestxid)
     (backend-storage-ipc-procarray/src/shmem_model.rs:429)
  -> debug_assert!(TransactionIdIsValid(xid))   <-- FAILS
```

`GetSnapshotData`'s GlobalVis-horizon computation calls
`FullXidRelativeTo(latest_completed, oldestxid)` with `oldestxid =
InvalidTransactionId` (0). `oldestxid` is `ShmemVariableCache->oldestXid`,
seeded by StartupXLOG/varsup during boot; in the multi-process boot it is still
invalid when the launcher takes its first snapshot. This is a procarray/varsup
shared-state init gap, NOT a launcher or `get_subscription_list` bug — the scan
is faithful and correctly drives into snapshot setup.

It is a `debug_assert!`, so a release build would skip the abort, but the
computed GlobalVis horizon would still be wrong; the real fix is to seed
`oldestXid` (and the other ShmemVariableCache xid bounds) during the
multi-process startup before the first snapshot. Same class as the
`startupxlog-blocked-on-subsystem-startup-seam-campaign` note (varsup /
xid-bound seeding).

The launcher child crash still triggers postmaster crash-recovery, so a live
`psql SELECT 1` does NOT yet answer over the socket. Next lane = WALL 1i:
seed `ShmemVariableCache->oldestXid` / xid bounds in the multi-process boot so
`GetSnapshotData` has a valid horizon.

## Non-regression

Single-user `SELECT 1;` on a raw C-initdb fixture reaches result-row emission
(prints the `----` separators) then panics at shmem-exit in the deferred
`ShutdownXLOG` driver (backend-access-transam-xlog/src/lib.rs:658) — a
pre-existing, unrelated shutdown-path wall (not touched by this change; no
launcher/subscription frame). The MEMORY-noted passing single-user smoke uses a
specially prepared "shut down" control template, not a raw C-initdb dir.
