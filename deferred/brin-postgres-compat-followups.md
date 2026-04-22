## Context

`USING brin` now supports PostgreSQL-shaped minmax storage, build/insert
maintenance, bitmap planning/execution, and metapage-backed
`pages_per_range` reload on reopen.

The branch still stops short of full PostgreSQL BRIN behavior for concurrent
summarization and crash recovery.

## Deferred

- placeholder-tuple VACUUM summarization flow, including the PostgreSQL retry
  loop around concurrent tuple replacement
- PostgreSQL-shaped BRIN WAL emission and replay for `CREATE_INDEX`,
  `INSERT`, `UPDATE`, `SAMEPAGE_UPDATE`, `REVMAP_EXTEND`, and `DESUMMARIZE`
- deferred SQL surface that was explicitly out of scope for the first BRIN
  milestone: `autosummarize`, BRIN helper SQL functions, bitmap AND/OR,
  expression indexes, partial indexes, `INCLUDE`, `UNIQUE`, and null-search
  scan keys

## Why Deferred

The current branch is enough to make BRIN usable for minmax indexes and to
exercise the planner/executor path, but the remaining work is mostly about
PostgreSQL parity under concurrency and crash recovery. That is a larger,
separate validation surface than the storage and planner slice already merged
here.

## Likely Approach

- port PostgreSQL's placeholder summarization path into
  `src/backend/access/brin/brin.rs` before broadening more SQL-visible BRIN
  features
- add BRIN WAL record codecs and recovery dispatch after the summarization
  path is stable, so replay semantics match the final page-update flow
- only then broaden deferred BRIN DDL and scan features
