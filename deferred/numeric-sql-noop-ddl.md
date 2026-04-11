# `numeric.sql` narrow DDL compatibility shims

To keep `numeric.sql` focused on numeric semantics, `pgrust` currently accepts two DDL forms as
deliberate no-ops:

- `CREATE [UNIQUE] INDEX ... ON ... (...)`
- `ALTER TABLE ... SET (parallel_workers = 4 [, ...])`

Current behavior:

- both statements parse and execute successfully
- no physical index is created
- uniqueness is not enforced
- reloptions are not stored anywhere
- planner behavior is unchanged

Why this exists:

- `numeric.sql` creates helper indexes and sets `parallel_workers`, but the file does not depend on
  real index behavior or reloption semantics for its expected output
- failing early on these statements blocks coverage of the actual numeric engine work

What PostgreSQL does differently:

- `CREATE INDEX` creates real catalog entries and physical index storage
- `UNIQUE` affects constraint/enforcement semantics
- `ALTER TABLE ... SET (...)` stores reloptions and exposes them through catalog state

Preferred follow-up:

- replace the no-op `CREATE INDEX` path with real index/catalog support
- replace the no-op `ALTER TABLE ... SET (...)` path with real reloption storage and catalog
  visibility
- remove the compatibility shims once those features exist
