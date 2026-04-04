# pgrust development notes

## Storage fork creation

Storage forks are created at startup by `Database::open` and by `CREATE TABLE`.
The insert path (`heap_insert_version`) assumes forks already exist and does NOT
create them on the fly.

Tests that use raw `BufferPool` instances (without going through `Database`) must
call a test-only `create_fork()` helper to set up the fork before inserting.
Fork creation code should only appear in test helpers, never on the hot insert path.
