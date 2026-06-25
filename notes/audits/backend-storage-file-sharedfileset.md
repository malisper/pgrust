# Audit: backend-storage-file-fileset::sharedfileset (sharedfileset.c)

Unit `backend-storage-file`, file `src/backend/storage/file/sharedfileset.c`.
Port lives in `crates/backend-storage-file-fileset/src/sharedfileset.rs`.
Independent re-derivation against the C and the c2rust rendering. fileset.c
(same crate) was audited separately; this audit covers only sharedfileset.c.

## Function inventory

C has 4 functions (1 static):

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `SharedFileSetInit` | sharedfileset.c:37-50 | sharedfileset.rs:61-90 | MATCH | `SpinLockInit`→`s_init_lock`; `refcnt=1`; `FileSetInit(&fs)` (fallible, `?`); `if (seg) on_dsm_detach(seg, SharedFileSetOnDetach, PointerGetDatum(fileset))` → `if seg.0 != 0` (`DsmSegmentHandle(0)` is the documented NULL sentinel) with the fileset's own address as the Datum arg. |
| `SharedFileSetAttach` | sharedfileset.c:55-77 | sharedfileset.rs:94-128 | MATCH | spinlock-guarded `refcnt==0 ? success=false : (++refcnt, success=true)`; release BEFORE the failure return (no lock across `?`); `ereport(ERROR, errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("could not attach to a SharedFileSet that is already destroyed"))` — exact message + SQLSTATE `55000`; then registers the same on-detach callback. |
| `SharedFileSetDeleteAll` | sharedfileset.c:82-86 | sharedfileset.rs:131-134 | MATCH | `FileSetDeleteAll(&fileset->fs)`. |
| `SharedFileSetOnDetach` (static) | sharedfileset.c:95-114 | sharedfileset.rs:142-170 | MATCH | recover `(SharedFileSet *) DatumGetPointer(datum)`; spinlock; `Assert(refcnt>0)`→`debug_assert!`; `--refcnt; if (==0) unlink_all=true`; release; `if (unlink_all) FileSetDeleteAll(&fs)`. C is `void` ("can't raise an error … runs in error cleanup paths"); the registered-callback type is `fn(DsmSegmentId, Datum) -> PgResult<()>` (the dsm-core contract), and the only `Err` `FileSetDeleteAll` can surface is OOM from path building — its IO failures are LOG-only via `walkdir(…, LOG)` in fd's `PathNameDeleteTemporaryDir`, matching C's non-raising behavior. |

## Constants / types

- `SharedFileSet` (`storage/sharedfileset.h`): `{ FileSet fs; slock_t mutex; int refcnt; }` — matches `types_storage::fileset::SharedFileSet`.
- `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE` = `55000` (verified in types-error).
- `PointerGetDatum`/`DatumGetPointer` modeled exactly as dsa.c's in-place control object does (`Datum::from_usize(addr)` / `datum.as_usize()`). The `&mut SharedFileSet` handed in is a real pointer into the DSM chunk (the parallel `shared_dsm_object` keystone resolves it from the in-segment address), so its address is the stable C `&pstate->fileset` pointer for the segment's lifetime.

## Seams / wiring

Owned seam crate (by c_source coverage): `backend-storage-file-sharedfileset-seams`.
All 3 declarations installed in `backend-storage-file-fileset::init_seams()`:
`SharedFileSetInit`, `SharedFileSetAttach`, `SharedFileSetDeleteAll` (each a thin
`set(super::sharedfileset::Fn)`). The seam signatures were corrected from the
earlier `()` placeholders to `PgResult<()>` (the C is fallible) and the single
consumer (`backend-executor-nodeHashjoin`, 3 call sites) updated in the same
change to propagate. seams-init recurrence_guard passes (owner wired).

Outward dependencies are direct (no seam): `backend-storage-ipc-dsm-core`
(`on_dsm_detach`, acyclic — dsm-core deps only `backend-storage-file-seams`, not
this crate), `backend-storage-lmgr-s-lock` (spinlock). `top_memory_context` is
the one seam (mcxt unported as a direct dep would cycle) — thin getter for the
`Mcx<'static>` the on-detach callback record is allocated in, the same pattern
shm_mq/dsa use. No registry, no ambient-global value seam, no lock held across
`?`.

## Verdict: PASS

All 4 functions MATCH; all 3 owned seams installed; no design findings.
