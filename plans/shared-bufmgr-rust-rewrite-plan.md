# Shared Buffer Manager Rust Rewrite Plan

## Summary

This plan records the first rewrite scope for PostgreSQL shared buffers in Rust.

Scope for v1:

- shared buffer manager core behavior
- direct storage-manager read/write boundary as an abstract trait
- deterministic multi-client test harness

Not in v1:

- local buffers
- real PostgreSQL shared-memory integration
- WAL/checkpointer/bgwriter behavior
- AIO and multi-block reads
- full `md.c` filesystem stack

## Deliverables

1. A detailed architecture document for the shared buffer manager and its `smgr` boundary.
2. A stub-model design document explaining the deterministic test harness.
3. A standalone Rust crate in `pgrust/` implementing the covered behavior.
4. API-level tests that verify the covered semantics and document the deferred ones.

## Implementation Milestones

### Phase 1

- Write the architecture and stub-model docs.
- Freeze the v1 coverage boundary and deferred-feature list.

### Phase 2

- Create a standalone Rust crate with:
  - explicit buffer tags and relation identity
  - a buffer pool model
  - a fake storage backend
  - deterministic read/write completion hooks

### Phase 3

- Implement covered shared-buffer behavior:
  - lookup by tag
  - page hit/miss behavior
  - default clock-sweep eviction
  - per-client pins and aggregate pin counts
  - dirty/valid/I/O state transitions
  - flush behavior and write retry semantics
  - relation invalidation for covered cases

### Phase 4

- Expand scenario tests until the Rust model is stable enough to serve as the spec for future integration work.
- Add a future C reference adapter in a later phase if stricter parity checks are needed.

## Acceptance Criteria

- `cargo test` passes in `pgrust/`.
- The crate exposes a narrow behavioral API for shared buffers.
- The docs clearly distinguish implemented behavior from deferred PostgreSQL behavior.
- The implementation preserves the following covered semantics:
  - cache hit after successful read
  - same-page read contention collapses to one canonical buffer
  - eviction skips pinned buffers
  - flush persists data and clears dirty on success
  - write failure retains dirty state

## Deferred Functionality

- `localbuf.c`
- bulk read / bulk write / vacuum ring strategies
- async read pipelines (`StartReadBuffers()` / `WaitReadBuffers()`)
- background writer and checkpointer interaction
- WAL flushing and redo/recovery paths
- exact lock, latch, shared-memory, and ResourceOwner integration
- `smgr` file segmentation, fsync choreography, unlink lifecycle, and writeback scheduling

