# Audit: common-variant-frontshlib-pgfnames-rmtree

Independent function-by-function audit, re-derived from the C and c2rust
renderings (not from the port's comments or self-review).

- C sources: `src/common/pgfnames.c`, `src/common/rmtree.c`
  (FRONTEND / shared-library variant: plain `opendir`/`readdir`/`closedir`,
  `opendir`-based `rmtree`).
- c2rust: `c2rust-runs/common-variant-frontshlib-pgfnames-rmtree/src/{pgfnames,rmtree}.rs`
- port: `crates/common-variant-frontshlib-pgfnames-rmtree/src/lib.rs`

## Function inventory & verdicts

| C function (file)                | Port location (lib.rs)          | Verdict |
|----------------------------------|---------------------------------|---------|
| `pgfnames` (pgfnames.c)          | `pgfnames` + helper `push_name` | MATCH   |
| `pgfnames_cleanup` (pgfnames.c)  | `pgfnames_cleanup`              | MATCH   |
| `rmtree` (rmtree.c)              | `rmtree` + `rmtree_in`          | MATCH   |

All three C function definitions are present. The c2rust run contains exactly
these three; no statics or inline helpers exist in either C file. No function is
MISSING, PARTIAL, DIVERGES, or SEAMED.

### pgfnames

- `opendir(path); if NULL { pg_log_warning(...); return NULL; }` → `fs::read_dir`
  Err arm returns `Ok(None)`. NULL-with-warning sentinel maps to `Ok(None)`; the
  warning is cosmetic. MATCH.
- `palloc(fnsize * sizeof(char*))`, `fnsize=200` → `vec_with_capacity_in(mcx, 200)`.
  Initial capacity 200 matches the C. MATCH.
- `while (errno=0, (file=readdir(dir)))` skipping `"."`/`".."` →
  `for entry in entries { if name != "." && name != ".." }`. MATCH.
- repalloc doubling on `numnames+1 >= fnsize` → `push_name` reserves fallibly
  when `len()==capacity()`, doubling, charged to `mcx`, OOM → Err (recoverable
  analog of the palloc-abort). The Rust list needs no NULL terminator (length
  carries the count), so reserving on `len==capacity` is equivalent to the C's
  `numnames+1 >= fnsize`. MATCH.
- `filenames[numnames++] = pstrdup(file->d_name)` → `PgString::new_in(mcx)` +
  `try_push_str`, pushed to the vec. pstrdup-into-context preserved. MATCH.
- read-error path: C exits the loop on `readdir`→NULL (errno set), warns, and
  **still returns the partial list**. Port `break`s then returns
  `Ok(Some(filenames))`. MATCH.
- `filenames[numnames] = NULL` terminator → unneeded (Vec length). `closedir`
  warning → implicit drop; never changes the result. MATCH.

### pgfnames_cleanup

C `pfree`s each name then the array. Port consumes the owned `PgVec<PgString>`;
each `PgString` and the spine reclaim their context charge on drop. Gated by the
`pgfnames_charge_released_after_drop` test (`ctx.used()==0` after drop). MATCH.

### rmtree / rmtree_in

- FRONTEND `OPENDIR` is `opendir` → `fs::read_dir`; Err arm warns + `return false`.
  MATCH.
- `palloc(sizeof(char*) * 8)`, cap 8 → `vec_with_capacity_in(mcx, 8)`; OOM →
  `return false`. Deferred-subdir list in a function-local `MemoryContext`,
  reclaimed on scope drop — matches the C per-level palloc/pfree and the
  one-open-fd-at-a-time invariant. MATCH.
- loop skips `"."`/`".."` (`continue`). MATCH.
- `snprintf(pathbuf, MAXPGPATH, "%s/%s", path, d_name)` → `format!("{path}/{}")`.
  MAXPGPATH truncation not modelled, but a path that would truncate in C fails
  the next syscall either way; behaviour on real trees identical. MATCH.
- `get_dirent_type(..., look_through_symlinks=false, LOG)` switch:
  - `PGFILETYPE_ERROR` (0): press on, result unchanged → `file_type()` Err arm,
    result unchanged. MATCH.
  - `PGFILETYPE_DIR` (3): defer + repalloc-double + `pstrdup(pathbuf)` →
    `ft.is_dir()` arm, `try_reserve(1)` (Err → result=false), `push(pathbuf)`.
    `look_through_symlinks=false` ⇒ lstat ⇒ a directory **symlink** is NOT
    `PGFILETYPE_DIR`; Rust `file_type()` is lstat-based (does not follow) so a
    symlink falls to unlink. Verified by
    `rmtree_unlinks_directory_symlink_without_following_it`. MATCH.
  - `default`: `if (unlink != 0 && errno != ENOENT) { warn; result=false; }` →
    `remove_file` Err arm `if e.kind() != NotFound { result=false }`
    (ENOENT==NotFound). MATCH.
- post-loop `if (errno != 0) { warn; result=false; }` → entry Err arm sets
  `result=false` then `break`s (same observable stop + failure). MATCH.
- `CLOSEDIR(dir)` → `entries` dropped at loop end. MATCH.
- recurse: `for i { if (!rmtree(dirnames[i], true)) result=false; pfree }` →
  `for dirname { fresh ctx; if !rmtree_in(...,true) result=false }`. Fresh
  per-level context (one fd at a time). MATCH.
- `if (rmtopdir) if (rmdir != 0) { warn; result=false; }` →
  `if rmtopdir { if remove_dir(path).is_err() { result=false } }`. MATCH.
- `pfree(dirnames)` → scope drop. `return result`. MATCH.

## Seam & wiring audit

Ownership is by C-source coverage; the unit's C files are `pgfnames.c` and
`rmtree.c` (frontend variant). There is no `pgfnames`-seams crate and no
frontend-`rmtree`-seams crate, and none is required:

- No outward seam calls: depends only on `std` (`fs`/`io`/`ffi`), `mcx`, and
  `types-error`. No dependency cycle exists.
- No inward seams: this frontend variant has no in-tree consumers (the only
  other `rmtree` is the **backend** `AllocateDir` variant owned by
  `backend-storage-file-fd`, exposed via `backend-storage-file-fd-seams::rmtree`
  — a separate unit; `pgfnames` has no in-tree callers). A pure leaf with no
  owned seam crate owns no seams, so an absent `init_seams()` is correct and
  `seams-init::init_all()` correctly does not reference it.

`recurrence_guard` (`every_seam_installing_crate_is_wired_into_init_all` and
`every_declared_seam_is_installed_by_its_owner`) passes — consistent with a
no-seam leaf.

## Design conformance

- Allocating functions take `Mcx` and return `PgResult` (`pgfnames`,
  `push_name`); OOM is a recoverable `PgError`, not an abort. OK.
- No invented opacity, no shared statics for per-backend globals, no ambient
  seams, no registry side tables, no locks across `?`, no unledgered divergence
  markers. OK.
- `rmtree`'s function-local `MemoryContext` per recursion level mirrors C's
  per-level palloc context and preserves the single-open-fd invariant. OK.

## Gates

- `cargo test -p common-variant-frontshlib-pgfnames-rmtree`: 8 passed, 0 failed.
- `cargo test -p seams-init`: 2 passed (both recurrence_guard checks).
- `cargo check --workspace`: clean (only pre-existing warnings in
  `backend-access-common-printtup`, unrelated to this unit).

## Verdict: PASS

Every function MATCH; zero seam findings; design-conformant. CATALOG row set to
`audited`.
