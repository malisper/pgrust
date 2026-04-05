# Add WAL delta records for UPDATE and DELETE

## Prerequisite

Checkpointing must be implemented first. Without checkpoints, the WAL
grows unbounded and replay starts from byte 0 every time. Adding more
delta record types increases WAL volume and replay time. Checkpoints
bound recovery to only the records since the last checkpoint.

## Current state

The WAL writer has three record types:
- `XLOG_FPI` (full page image) — used for all modifications
- `XLOG_HEAP_INSERT` — insert delta (tuple data only, ~100 bytes vs 8KB FPI)
- `XLOG_XACT_COMMIT` — transaction commit

UPDATE and DELETE currently go through `write_page_image` which writes a
full 8KB FPI of the modified page. This is correct but wasteful — a
single-column update or row deletion could be represented as a ~50 byte
delta record instead.

## What to add

### XLOG_HEAP_DELETE delta
- Record: header + block header + offset_number (2 bytes)
- Replay: set xmax on the tuple at the given offset, mark HEAP_XMAX_COMMITTED
- PostgreSQL equivalent: `heap_xlog_delete` in `heapam_xlog.c`

### XLOG_HEAP_UPDATE delta
- Record: header + block header + old_offset (2) + new_offset (2) + new_tuple_data
- Replay: mark old tuple's xmax, insert new tuple at new_offset
- PostgreSQL equivalent: `heap_xlog_update` in `heapam_xlog.c`
- Note: HOT updates (same page) can be even more compact

## Torn page safety with checkpoints

Currently, replay always starts from byte 0 of the WAL, so every page
has at least one FPI before any deltas.  FPIs are applied unconditionally
(ignoring page LSN) to protect against torn pages.

Once checkpoints are added and WAL is truncated, a delta record may be
the first record for a page after the checkpoint.  In that case the
on-disk page was cleanly flushed by the checkpoint (not torn), so the
LSN check on deltas is sufficient — skip if `page_lsn >= record_lsn`.

However, the first modification to each page after a checkpoint must
still write an FPI (matching PostgreSQL's `full_page_writes` behavior).
This ensures that if a crash happens before the next checkpoint, recovery
has an FPI to restore the page from, protecting against torn writes that
occurred between the checkpoint and the crash.  The WAL writer's
`pages_with_image` tracking needs to be reset at each checkpoint.

## Impact

For UPDATE/DELETE-heavy workloads, this would reduce WAL volume by ~99%
per operation (50 bytes vs 8KB). This directly improves:
- WAL write throughput
- Recovery/replay time
- Disk usage for WAL files
