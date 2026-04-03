# PostgreSQL API Boundaries

This document maps the major subsystem boundaries in PostgreSQL as they exist in the source tree in this checkout. It is not describing a stable external ABI. It is describing the internal seams that the PostgreSQL backend actually uses to move a statement from protocol input to durable storage.

I did two passes to build this:

1. External architecture references, mainly the PostgreSQL docs on query flow, executor, and access methods.
2. Source inspection of this checkout to anchor those boundaries to actual files, structs, and entrypoints.

## External References Used First

- PostgreSQL docs on the path of a query and internals: <https://www.postgresql.org/docs/current/internals.html>
- PostgreSQL docs on the executor: <https://www.postgresql.org/docs/18/executor.html>
- PostgreSQL docs on the index access method interface: <https://www.postgresql.org/docs/current/index-functions.html>

## The Big Picture

For a normal optimizable SQL statement, the core pipeline is:

1. `tcop/postgres.c`: protocol input and statement lifecycle
2. `parser/`: raw parse tree creation
3. `parser/` + `rewrite/`: parse analysis and rule rewriting
4. `optimizer/`: planning into a `PlannedStmt`
5. `executor/`: execution against tables, indexes, snapshots, and functions
6. `access/`, `storage/`, `access/transam/`: data access, buffering, WAL, transactions

The principal handoff objects are:

- SQL text -> `List *` of `RawStmt`
- `RawStmt` -> `Query`
- `Query` -> `List *` of rewritten `Query`
- `Query` -> `PlannedStmt`
- `PlannedStmt` -> `QueryDesc` / executor state
- executor tuple flow -> `TupleTableSlot`
- relation access -> `Relation`
- storage access -> `Buffer`, `Page`, `SMgrRelation`
- MVCC visibility -> `Snapshot`

## 1. Protocol / Traffic Cop Boundary

The "traffic cop" layer lives in [`src/backend/tcop/postgres.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/tcop/postgres.c) and related `tcop/` files.

Primary responsibilities:

- accept frontend protocol messages
- manage per-statement transaction command boundaries
- choose simple-query vs extended-query execution paths
- invoke parse, rewrite, plan, and portal execution

Key entrypoints:

- `PostgresMain()`
- `exec_simple_query()`
- `pg_parse_query()`
- `pg_analyze_and_rewrite_*()`
- `pg_plan_query()` / `pg_plan_queries()`

Boundary contract:

- Input: SQL text and protocol state
- Output to lower layers: parsed, rewritten, or planned statement trees
- Output to client side: `DestReceiver` and protocol messages

Important rule:

- `tcop` owns statement lifecycle and orchestration. Lower layers do not own protocol semantics.

## 2. Parser Boundary

The parser boundary converts text into raw syntax trees.

Key source:

- [`src/backend/tcop/postgres.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/tcop/postgres.c)
- [`src/backend/parser/`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/parser)

Key function:

- `pg_parse_query()` calls `raw_parser()`

Boundary contract:

- Input: SQL string
- Output: `List *` of `RawStmt`

What this layer does not do:

- no catalog-driven semantic resolution
- no type resolution
- no planning
- no execution

The parser boundary is intentionally narrow: it produces syntax, not semantics.

## 3. Parse Analysis and Rewrite Boundary

This is the first semantic boundary.

Key functions in [`src/backend/tcop/postgres.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/tcop/postgres.c):

- `pg_analyze_and_rewrite_fixedparams()`
- `pg_analyze_and_rewrite_varparams()`
- `pg_analyze_and_rewrite_withcb()`
- `pg_rewrite_query()`

Key lower modules:

- [`src/backend/parser/`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/parser)
- [`src/backend/rewrite/`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/rewrite)

Boundary contract:

- Input: `RawStmt`
- Output after analysis: `Query`
- Output after rewrite: `List *` of `Query`

What crosses this boundary:

- resolved names
- resolved types
- range table entries
- semantic query shape
- rewrite expansions, including rule-produced extra queries

Why this matters:

- `Query` is the first representation that the rest of the system can treat as semantically meaningful.
- rewrite can expand one input statement into multiple `Query` trees, so callers above this boundary must already be list-aware.

## 4. Planner / Optimizer Boundary

This boundary turns semantic queries into executable plans.

Key API:

- [`src/include/optimizer/optimizer.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/optimizer/optimizer.h)
- [`src/include/optimizer/planner.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/optimizer/planner.h)

Key functions:

- `planner()`
- `pg_plan_query()`
- `pg_plan_queries()`
- hook: `planner_hook`
- hook: `create_upper_paths_hook`

Boundary contract:

- Input: `Query`
- Output: `PlannedStmt`

Important detail:

- `pg_plan_query()` asserts that an active snapshot exists. The planner is not purely abstract; it may execute catalog lookups and user-defined functions during planning.

Planner boundary invariants:

- callers outside the planner should mostly treat `PlannerInfo` as opaque
- the public non-planner API is intentionally concentrated in `optimizer/optimizer.h`
- planner extensions generally enter through hooks, not by reaching into random optimizer internals

## 5. Utility Command Boundary

Utility statements do not follow the normal planner/executor path.

Key API:

- [`src/backend/tcop/utility.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/tcop/utility.c)
- [`src/include/tcop/utility.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/tcop/utility.h)

Key functions:

- `ProcessUtility()`
- `standard_ProcessUtility()`
- hook: `ProcessUtility_hook`

Boundary contract:

- Input: `PlannedStmt` whose `commandType` is `CMD_UTILITY`
- Dispatch by `nodeTag(parsetree)` on `utilityStmt`

Key distinction:

- normal DML/SELECT: parse -> rewrite -> plan -> executor
- utility: parse -> analysis -> wrapped `PlannedStmt` -> `ProcessUtility`

This is one of the most important system boundaries in PostgreSQL. If code assumes everything becomes an executor plan, it will be wrong.

## 6. Portal Boundary

Portals are the execution-state boundary between planning and actually running a statement.

Key API:

- [`src/include/utils/portal.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/utils/portal.h)
- [`src/backend/tcop/pquery.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/tcop/pquery.c)

Key object:

- `Portal`

Boundary contract:

- Input: source text, `List *` of `PlannedStmt`, params, query environment
- Output: managed execution lifecycle, cursor semantics, suspension/resume where allowed

Why it exists:

- protocol-level portals and SQL cursors need state beyond a bare `PlannedStmt`
- execution can be incremental for some strategies and all-at-once for others

Important rule:

- portal strategy is a semantic boundary. `PORTAL_ONE_SELECT` can suspend; multi-query and many utility paths cannot.

## 7. Executor Boundary

The executor consumes plans and produces effects and tuples.

Key API:

- [`src/include/executor/executor.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/executor/executor.h)
- [`src/backend/executor/`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/executor)

Key functions:

- `ExecutorStart()`
- `ExecutorRun()`
- `ExecutorFinish()`
- `ExecutorEnd()`

Executor hooks:

- `ExecutorStart_hook`
- `ExecutorRun_hook`
- `ExecutorFinish_hook`
- `ExecutorEnd_hook`
- `ExecutorCheckPerms_hook`

Boundary contract:

- Input: `QueryDesc` with a `PlannedStmt`, snapshot state, destination, params
- Output: rows via `DestReceiver`, DML side effects, trigger firing, index updates

Primary data abstraction:

- `TupleTableSlot`

This is a major boundary. Executor nodes should pass tuples through slots, not by exposing table-specific physical tuple formats everywhere.

## 8. Function Manager Boundary

The function manager is the dynamic call boundary for SQL-callable functions.

Key API:

- [`src/include/fmgr.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/fmgr.h)

Key objects:

- `FmgrInfo`
- `FunctionCallInfo`
- `PGFunction`

Boundary contract:

- Input: OID-resolved callable plus arguments in `FunctionCallInfo`
- Output: `Datum` plus null/result metadata

Why this matters:

- executor, planner, expressions, operators, and index support functions all converge on this calling convention
- this is one of the cleanest internal API boundaries in PostgreSQL

Practical rule:

- if a subsystem needs to call a SQL-visible function, it should normally do it via fmgr, not by special-casing implementation details

## 9. Catalog / Relation Cache Boundary

PostgreSQL does not let most subsystems manipulate raw catalog tuples directly all the time. The normal boundary is through caches and descriptors.

Key APIs:

- [`src/include/utils/syscache.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/utils/syscache.h)
- [`src/include/utils/relcache.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/utils/relcache.h)

Key objects and functions:

- `Relation`
- `RelationIdGetRelation()`
- `RelationClose()`
- `SearchSysCache*()`
- `ReleaseSysCache()`

Boundary contract:

- system catalogs are storage
- syscache and relcache are the access APIs most upper layers should use

Why this boundary exists:

- centralizes metadata lookup
- supports invalidation
- hides catalog storage details from executor/planner/command code

Rule of thumb:

- upper layers usually want `Relation` or syscache lookup results, not direct heap scans of `pg_class`, `pg_attribute`, and friends

## 10. Transaction / Command / Snapshot Boundary

MVCC state and transaction state are their own subsystem, not just a property of storage.

Key documentation in source:

- [`src/backend/access/transam/README`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/access/transam/README)

Key API:

- [`src/include/utils/snapmgr.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/utils/snapmgr.h)

Key functions:

- `GetTransactionSnapshot()`
- `GetLatestSnapshot()`
- `PushActiveSnapshot()`
- `PopActiveSnapshot()`
- `RegisterSnapshot()`
- `SnapshotSetCommandId()`

Boundary contract:

- upper layers ask transaction/snapshot code for visibility state
- table/index/executor code consumes snapshots to decide what is visible

Important distinction:

- transaction control and snapshot visibility are closely related but not identical boundaries
- the `xact.c` layer manages transaction state machines and command boundaries
- snapshot manager exposes visibility state for readers

Critical rule:

- code above storage should not invent its own visibility semantics; it should consume `Snapshot`

## 11. Table Access Method Boundary

This is the primary abstraction boundary for table storage behavior.

Key API:

- [`src/include/access/tableam.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/access/tableam.h)

Key concepts:

- `TableAmRoutine`
- `TableScanDesc`
- `TupleTableSlot`
- wrappers such as `table_beginscan()`, `table_scan_getnextslot()`, `table_tuple_insert()`, `table_tuple_update()`, `table_index_build_scan()`

Boundary contract:

- callers above table AM use table-level operations
- concrete table implementations supply the callback table

What the table AM hides:

- physical tuple layout details
- scan mechanics
- update/delete/lock behavior details
- visibility implementation details needed by the table format

What still leaks through:

- MVCC semantics
- relation metadata
- buffer/page-level concerns in low-level access code

This is the main pluggable boundary for heap-like storage.

## 12. Index Access Method Boundary

This is the parallel abstraction boundary for indexes.

Key APIs:

- [`src/include/access/amapi.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/access/amapi.h)
- [`src/include/access/genam.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/access/genam.h)
- [`src/backend/access/index/indexam.c`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/access/index/indexam.c)

Key concepts:

- `IndexAmRoutine`
- wrappers such as `index_insert()`, `index_beginscan()`, `index_getnext_slot()`, `index_endscan()`
- `GetIndexAmRoutine()`

Boundary contract:

- generic index code and upper layers call the generic wrappers
- the wrappers dispatch into the selected index AM's callback table

What crosses the boundary:

- index keys and null flags
- heap TID
- uniqueness policy
- scan keys and ordering keys

Important relationship:

- table AM owns table tuple semantics
- index AM owns index tuple semantics
- coordination points exist where both participate, such as index builds and index-driven tuple deletion

## 13. Buffer Manager Boundary

The buffer manager is the page-cache boundary between access methods and physical storage.

Key API:

- [`src/include/storage/bufmgr.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/storage/bufmgr.h)

Key functions:

- `ReadBuffer()`
- `ReadBufferExtended()`
- `ReleaseBuffer()`
- `MarkBufferDirty()`
- `LockBuffer()`

Boundary contract:

- callers ask for logical relation/fork/block pages
- buffer manager returns shared buffer handles and coordinates concurrency, pinning, and dirty-state tracking

Design rule:

- most table/index code should go through the buffer manager, not directly to the storage manager

## 14. Storage Manager Boundary

The storage manager is the low-level physical file boundary.

Key API:

- [`src/include/storage/smgr.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/storage/smgr.h)

Key object:

- `SMgrRelation`

Key functions:

- `smgropen()`
- `smgrreadv()`
- `smgrwritev()`
- `smgrextend()`
- `smgrtruncate()`
- `smgrnblocks()`

Boundary contract:

- input: relation file identity plus fork/block operations
- output: actual file I/O

Important rule:

- `smgr` is not the normal API for high-level SQL execution code
- it is the physical storage layer under buffer management and some recovery/bootstrap code

## 15. WAL / Recovery Boundary

Durability is a separate subsystem boundary from data access.

Key source area:

- [`src/backend/access/transam/`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/access/transam)

Key functions:

- `XLogInsert()`
- `XLogBeginInsert()`
- `XLogRegister*()` in xlog insert code

Boundary contract:

- access methods and transactional subsystems describe logical/physical changes as WAL records
- WAL machinery serializes, reserves, inserts, and later replays them

Important separation:

- changing a page in shared buffers is not the same as making it durable
- WAL is the durability protocol between access methods and crash recovery

In practice:

- low-level access code must respect the ordering contract between page modification, LSN assignment, and WAL insertion

## 16. Background Process / Postmaster Boundary

The backend process is not the whole server. There is a separate process-management boundary.

Key source area:

- [`src/backend/postmaster/`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/backend/postmaster)

Examples:

- postmaster
- checkpointer
- autovacuum
- bgwriter
- walwriter

Boundary contract:

- backend-local execution code communicates with cluster-wide services through shared memory, latches, locks, and WAL, not by direct in-process calls

This matters when reasoning about "systems" in PostgreSQL: many responsibilities are intentionally not in the backend proper.

## Most Important Cross-System Objects

If I had to summarize PostgreSQL's subsystem boundaries by object type rather than by directory, the key objects are:

- `RawStmt`: syntax boundary
- `Query`: semantic query boundary
- `PlannedStmt`: plan boundary
- `Portal`: execution lifecycle boundary
- `QueryDesc`: executor invocation boundary
- `TupleTableSlot`: tuple interchange boundary
- `Relation`: relation metadata boundary
- `Snapshot`: visibility boundary
- `TableAmRoutine`: table storage abstraction boundary
- `IndexAmRoutine`: index abstraction boundary
- `Buffer`: page-cache boundary
- `SMgrRelation`: physical storage boundary
- `FmgrInfo` / `FunctionCallInfo`: callable function boundary

## Extension and Customization Boundaries

The main intentional extension seams are:

- planner hooks in [`src/include/optimizer/planner.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/optimizer/planner.h)
- executor hooks in [`src/include/executor/executor.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/executor/executor.h)
- utility hook in [`src/include/tcop/utility.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/tcop/utility.h)
- table access methods in [`src/include/access/tableam.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/access/tableam.h)
- index access methods in [`src/include/access/amapi.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/access/amapi.h)
- SQL-callable functions through [`src/include/fmgr.h`](/Users/malisper/workspace/work/postgres-rewrite/postgres/src/include/fmgr.h)

These are much more real API boundaries than random internal helper functions.

## Practical Boundary Rules

These rules are a good approximation of how the codebase is intended to be navigated:

- Parse code should output trees, not touch storage.
- Rewriter should transform `Query` trees, not execute them.
- Planner should output `PlannedStmt`, not perform execution work.
- Utility statements bypass normal executor planning and go through `ProcessUtility`.
- Executor should exchange tuples via `TupleTableSlot`.
- Metadata consumers should prefer relcache/syscache over raw catalog heap access.
- Table code should prefer table AM wrappers over assuming heap internals.
- Index code should prefer generic index wrappers over assuming a specific AM.
- Access methods should normally use buffer manager APIs, not raw storage manager calls.
- Durability should go through WAL APIs, not ad hoc filesystem logic.
- Visibility decisions should be snapshot-driven, not reimplemented locally.

## What Seems Most Fundamental

The most consequential boundaries in PostgreSQL are not directory boundaries. They are representation boundaries:

- text -> `RawStmt`
- `RawStmt` -> `Query`
- `Query` -> `PlannedStmt`
- `PlannedStmt` -> executor state
- executor tuples -> `TupleTableSlot`
- logical relation access -> `Relation` / table AM / index AM
- physical page access -> `Buffer`
- physical file access -> `SMgrRelation`
- visibility -> `Snapshot`
- durability -> WAL record APIs

That is the architecture to preserve if the goal is to reimplement pieces cleanly.

