# smgr — Intentionally Deferred Features

The following PostgreSQL storage manager features were not implemented in the
first pass of `pgrust/src/smgr.rs`. Each entry explains what the feature is,
why it was deferred, and what would be needed to add it.

---

## 1. Async I/O (`smgr_startreadv` / `PgAioHandle`)

**What it is:** PostgreSQL 17 introduced a kernel-AIO subsystem. Storage
managers expose `smgr_startreadv` to initiate non-blocking reads; completion
is signalled via `PgAioHandle` callbacks.

**Why deferred:** The behavioral model only needs correct sequential semantics.
AIO adds significant complexity (completion queues, callback dispatch, handle
lifecycle) with no benefit for unit testing.

**To add:** Introduce an async variant of `read_block` (e.g., returning a
`Future` or an `AioHandle` token), and a poll/complete step. Likely warrants
its own module.

---

## 2. Prefetch (`smgr_prefetch`)

**What it is:** An advisory hint to the OS to start reading ahead before the
data is needed (`posix_fadvise(POSIX_FADV_WILLNEED)` on Linux).

**Why deferred:** The OS page cache handles readahead naturally. Prefetch is a
performance hint, not a correctness requirement. Not in the `StorageManager`
trait.

**To add:** Add `fn prefetch(&mut self, rel, fork, block, nblocks)` to the
trait. Implementation calls `posix_fadvise` (Linux) or is a no-op elsewhere.

---

## 3. `smgr_maxcombine`

**What it is:** Returns the maximum number of blocks that can be submitted as
a single vectored I/O operation. Used by the AIO layer to size its iovec
arrays.

**Why deferred:** No AIO layer exists yet. Not in the `StorageManager` trait.

**To add:** Add `fn max_combine(&self, rel, fork, block) -> u32` once AIO is
in place. The md.c implementation returns `min(MAX_IO_COMBINE_LIMIT,
RELSEG_SIZE - block % RELSEG_SIZE)`.

---

## 4. ~~Deferred fsync (`smgr_registersync`)~~ — **Implemented as immedsync**

**What it is:** Instead of fsyncing immediately on every write, PostgreSQL
accumulates "dirty segment" registrations and flushes them all at checkpoint
time via a sync queue (`sync.c`). This amortises fsync cost across many writes.

**Status:** `write_block` and `extend` always call `file.sync_data()`,
ignoring `skip_fsync`. Without WAL there is no crash-recovery path, so
honouring `skip_fsync=true` would silently risk data loss. The parameter is
accepted for API compatibility but has no effect until WAL is implemented.

**Still not implemented:** Checkpoint-level batched sync (`smgr_registersync` /
`sync.c` queue). To add: maintain a `BTreeSet<SegKey>` of pending-sync
segments, add `fn register_sync(&mut self, rel, fork)` to enqueue them, and
`fn checkpoint_sync(&mut self)` to drain the queue via `sync_data` on each.

---

## 5. Non-default tablespace paths

**What it is:** PostgreSQL maps non-zero `spc_oid` values to paths under
`$PGDATA/pg_tblspc/<spc_oid>/PG_<major>_<catalog_version>/<db_oid>/`.

**Why deferred:** All test relations use the default tablespace (`spc_oid = 0`,
mapped to `base/<db_oid>/`). Supporting custom tablespaces requires knowing the
Postgres version and catalog version string at runtime.

**To add:** Add a tablespace-to-path resolver (a `HashMap<u32, PathBuf>` or a
callback) to `MdStorageManager`. The default entry maps 0 → `base_dir`.
`segment_path()` calls the resolver before constructing the final path.

---

## 6. WAL/recovery semantics (`is_redo`, `InRecovery`)

**What it is:** During WAL replay, certain behaviors change:
- `mdexists` skips the `mdclose` before re-opening (relations are already
  closed when dropped in recovery).
- `mdcreate` with `is_redo=true` tolerates pre-existing files.
- `mdunlink` with `is_redo=true` removes immediately (no deferred unlink).
- `TablespaceCreateDbspace` is called with `isRedo=true`.

**Why deferred:** `is_redo` is accepted in `create()` (allows pre-existing
files) but otherwise ignored. A full recovery mode would need a flag on the
storage manager and adjusted behaviour in `exists`, `unlink`, and `create`.

**To add:** Add an `in_recovery: bool` field to `MdStorageManager` and thread
the `is_redo` flag through the affected methods.

---

## 7. `smgr_fd` — raw file descriptor exposure

**What it is:** Returns the OS-level file descriptor for a specific block,
along with the byte offset within that file. Used by the AIO subsystem to
submit `io_uring` / `preadv` operations directly.

**Why deferred:** Exposes too much internal state for the behavioral model, and
requires AIO to be useful.

**To add:** Expose a method that returns a `RawFd` (Unix) or equivalent, once
the AIO layer exists.

---

## 8. Inactive segment retention after truncate

**What it is:** PostgreSQL's `mdtruncate` leaves truncated segment files in
place as zero-length files (rather than removing them) for the main fork of
permanent relations. This prevents a truncated relation's file number from
being reused before the next checkpoint, protecting against a crash/replay
scenario where a new relation gets the same file number.

**Why deferred:** Safe to omit for the standalone behavioral model (no
crash/replay scenario). Our `truncate` simply removes excess segment files.

**To add:** For the main fork of non-temp relations, truncate excess segments
to 0 bytes but leave the files. Track which segments are "inactive" so they
are skipped by `nblocks`. Remove them (or let the OS reclaim them) only after
checkpoint confirms it is safe.

---

## 9. `PROCSIGNAL_BARRIER_SMGRRELEASE`

**What it is:** A process signal that forces a backend to immediately close all
file descriptors held by `SMgrRelation` objects, without destroying the objects
themselves. Used when a relation is being dropped and the dropping backend needs
to ensure no other backend holds open FDs before it removes the file.

**Why deferred:** Single-process model; no inter-process signalling needed.

**To add:** In a multi-process context, implement a signal handler that calls
`close()` on all open segments, clearing `open_segs` without dropping the
relation entries themselves. Requires the SMgrRelation lifetime model (item 10).

---

## 10. SMgrRelation hash / pin system

**What it is:** PostgreSQL's `smgr.c` maintains a process-global hash table of
`SMgrRelation` objects, one per `RelFileLocatorBackend`. Objects are pinned by
the relcache to extend their lifetime across transactions; unpinned objects are
destroyed at `AtEOXact_SMgr()`.

**Why deferred:** The Rust model is single-session and uses a simple `HashMap`
of open file handles keyed by `(rel, fork, segno)`. No lifetime management
beyond the storage manager's own lifetime is needed.

**To add:** A `SmgrRelation` wrapper type with a pin count; a process-global
registry; and transaction-end cleanup. Likely needed once the buffer manager
and smgr are integrated.
