# Storage Layer — Remaining Work

This note records what is still left in the storage layer after the first
`smgr` / `md` implementation and buffer-manager integration.

The point is to keep the backlog explicit so we can move on to tuple-format
work without pretending storage is "done done".

## Status

The current storage layer is good enough for the next subsystem:

- `smgr` / `md` exists and is integrated with the buffer manager
- unit tests pass
- integration-style tests pass
- end-to-end binaries pass
- basic crash-oriented behaviors are covered

That means storage is complete enough for behavioral-model work on tuple
format and heap access.

## Non-blocking follow-up work

These should be done, but they are not a reason to delay tuple-format work.

### 1. Write down the intended long-term crate layout

We do not need to mirror PostgreSQL's file layout exactly right now, but we
should document the target shape somewhere in-repo before more modules are
added.

Minimum outcome:

- describe the intended high-level tree
- state what should remain grouped by subsystem
- make clear which parts are intentionally not mirrored 1:1

### 2. Reconcile storage docs with current truncate behavior

The old deferred-features note said excess segments were removed entirely.
The current code and tests are closer to PostgreSQL's inactive-segment model
for truncation.

Minimum outcome:

- update docs so they describe the code that actually exists
- call out any remaining mismatch with PostgreSQL semantics explicitly

### 3. Decide whether `pgrust::smgr` stays as a compatibility alias

The canonical code now lives under `storage/smgr`, but `lib.rs` re-exports
`smgr` for compatibility.

Minimum outcome:

- either keep that alias intentionally
- or remove it later once call sites have fully moved

### 4. Tighten semantics around recovery / checkpoint behavior

The behavioral model has some crash-oriented coverage, but it is still far
from PostgreSQL's full WAL/recovery contract.

This matters eventually, but not before tuple layout / heap access.

## Deferred features that are still out of scope

These remain intentionally deferred and are tracked in
`plans/smgr-deferred-features.md`:

- async I/O
- richer prefetch / I/O-combine behavior
- deferred fsync queueing
- non-default tablespace paths
- full WAL / redo semantics
- raw FD exposure for AIO-oriented callers
- full `SMgrRelation` hash / pin lifecycle
- multi-process signal-driven handle release

## Practical recommendation

Proceed to tuple-format work now.

The storage layer is sufficiently real that tuple/page format work will sit on
top of an actual disk-backed boundary rather than a fake stub. That is the
right next dependency edge.
