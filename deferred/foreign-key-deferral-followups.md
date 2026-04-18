## Context

`ALTER TABLE ... ALTER CONSTRAINT ... DEFERRABLE | NOT DEFERRABLE |
INITIALLY DEFERRED | INITIALLY IMMEDIATE` now updates foreign key catalog flags
and the runtime honors `DEFERRABLE INITIALLY DEFERRED` constraints by checking
them at commit instead of at each individual DML statement.

That is enough for the current milestone, but the implementation still stops
short of full PostgreSQL deferrability behavior.

## Deferred

- `SET CONSTRAINTS` support, including switching deferrable constraints between
  immediate and deferred mode within an open transaction
- a more incremental deferred-check runtime instead of commit-time rescans of
  every affected foreign key constraint

## Why Deferred

The current runtime is correct for the new `ALTER CONSTRAINT` behavior that
ships with this milestone, and it covers the highest-value `INITIALLY DEFERRED`
path without forcing broader executor and transaction-state work into the same
change.

`SET CONSTRAINTS` needs transaction-local mode tracking and PostgreSQL-shaped
error timing. Incremental deferred checking needs more than catalog metadata: it
needs a pending-check representation tied to row-level DML, commit, and later
savepoint semantics.

## Likely Approach

- add transaction-local constraint mode state so `DEFERRABLE INITIALLY
  IMMEDIATE` can be deferred after `SET CONSTRAINTS`
- store pending foreign key checks as row/key work items instead of only
  recording affected constraint OIDs
- validate those pending checks at commit, and later teach the same state model
  about savepoints when transactional nesting grows
