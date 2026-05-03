# pgrust Antithesis system summary

This is the shortest useful description of what Antithesis should test in
pgrust and what we need locally before any platform handoff.

## Architecture in one page

pgrust is a single-node PostgreSQL-style database server written in Rust.
Client traffic enters through the libpq / tcop path, then flows through:

- `src/pgrust/`: server, session, cluster, and per-database orchestration
- `src/backend/parser/` + `src/backend/optimizer/`: SQL parsing, binding, and plan construction
- `src/backend/executor/`: expression evaluation, plan execution, tuple/value I/O, aggregates
- `src/backend/catalog/`: table/type metadata and catalog mutations
- `src/backend/access/` + `src/backend/storage/`: heap/index access, pages, buffer/storage, locks

The important state for Antithesis is not just query results. It is the
combination of on-disk relation state, catalog metadata, transaction/lock
state, session state, and the wire-protocol lifecycle around reconnects and
errors. Bugs here often require a bad interleaving or a kill/restart boundary,
which is exactly where ordinary unit tests are weakest.

## Failure modes worth hunting

The first Antithesis campaign should target bugs that need concurrency, crash
timing, or restart to appear:

- catalog vs storage split-brain: a relation or index is visible in metadata
  but missing, stale, duplicated, or half-applied on disk
- transaction visibility mistakes: concurrent sessions observe rows too early,
  too late, twice, or not at all
- lock / wait-state bugs: blocked sessions never unblock, unblock too early, or
  get the wrong error after cancellation or peer failure
- DDL + DML races: `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, or index work
  interleaves badly with reads/writes from other sessions
- restart cleanup bugs: reconnecting after kill/restart leaves partial temp
  state, leaked prepared state, orphaned files, or broken catalog entries
- protocol cleanup bugs: a client disconnect, parse/bind/execute failure, or
  `COPY` interruption leaves the session in the wrong transaction state

## What Antithesis is especially good at for pgrust

Antithesis is not mainly for parser panics or ordinary SQL correctness. We have
fuzzing, targeted Rust tests, and planned SQLancer/Hegel work for that. The
high-value Antithesis use cases are:

- forcing rare interleavings across multiple client sessions without rewriting
  the server into a simulator
- injecting crashes or network/process faults near state transitions that are
  hard to hit on purpose
- replaying one bad run deterministically so a storage or cleanup bug is
  debuggable instead of "one in a million"
- checking cross-layer invariants while the system is live, not only after a
  failed assertion in a unit test

For pgrust, that means durability/recovery-adjacent bugs, session cleanup bugs,
and concurrency bugs are the first targets.

## Minimum viable local-prep checklist

Before any real Antithesis run, the repo should support a cheap local dry run:

1. Keep this doc as the architecture/property brief and keep
   `antithesis/README.md` aligned with it.
2. Create the minimal `antithesis/` layout:
   `Dockerfile`, `setup-complete.sh`, `config/docker-compose.yaml`,
   `test/main/`.
3. Boot one pgrust server container plus one workload container locally and
   prove the workload can connect, create state, and reconnect.
4. Emit basic lifecycle events and invariants through `antithesis-sdk-rust`
   with `ANTITHESIS_SDK_LOCAL_OUTPUT` enabled so the same assertions run off
   platform.
5. Start with one small multi-session workload: connect, create table, insert,
   update, delete, simple DDL, forced disconnect/reconnect, then verify the
   final rows and metadata still agree.
6. Have one rerunnable command that proves the whole shape works locally before
   any platform submission.

If we cannot do step 6, the Antithesis setup is still too vague.
