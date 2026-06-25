# Audit: backend-storage-file-buffile

- **Unit:** `backend-storage-file` (this port: `src/backend/storage/file/buffile.c`, PostgreSQL 18.3)
- **Crate:** `backend-storage-file-buffile`
- **Branch:** `port/backend-storage-file-buffile`
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS**

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Re-derived from the C source
(`../pgrust/postgres-18.3/src/backend/storage/file/buffile.c`), the c2rust
rendering (`../pgrust/c2rust-runs/backend-storage-file/src/buffile.rs`), and the
Rust port; the port's own self-review was ignored.

Scope note: the catalog unit `backend-storage-file` bundles six C files
(`buffile.c, copydir.c, fd.c, fileset.c, reinit.c, sharedfileset.c`). This branch
ports **only `buffile.c`** (the others remain `todo` for the unit per CATALOG).
The audit therefore covers every function in `buffile.c`; `fd.c`/`fileset.c`/etc.
are out of scope and reached through their own (unported) owners' seam crates.

## 1. Function inventory

Every function definition in `buffile.c` (25 total), cross-checked against the
c2rust rendering (which kept all 25) and the port.

| # | C function (buffile.c) | C line | Port location (lib.rs) | Verdict |
|---|---|---|---|---|
| 1 | `makeBufFileCommon` | 117 | `makeBufFileCommon` :105 | MATCH |
| 2 | `makeBufFile` | 138 | `makeBufFile` :125 | MATCH |
| 3 | `extendBufFile` | 156 | `extendBufFile` :139 | MATCH |
| 4 | `BufFileCreateTemp` | 192 | `BufFileCreateTemp` :190 | MATCH |
| 5 | `FileSetSegmentName` | 221 | `FileSetSegmentName` :162 | MATCH |
| 6 | `MakeNewFileSetSegment` | 230 | `MakeNewFileSetSegment` :171 | MATCH |
| 7 | `BufFileCreateFileSet` | 266 | `BufFileCreateFileSet` :208 | MATCH |
| 8 | `BufFileOpenFileSet` | 290 | `BufFileOpenFileSet` :231 | MATCH |
| 9 | `BufFileDeleteFileSet` | 363 | `BufFileDeleteFileSet` :283 | MATCH |
| 10 | `BufFileExportFileSet` | 393 | `BufFileExportFileSet` :314 | MATCH |
| 11 | `BufFileClose` | 411 | `BufFileClose` :331 | MATCH |
| 12 | `BufFileLoadBuffer` | 433 | `BufFileLoadBuffer` :347 | MATCH |
| 13 | `BufFileDumpBuffer` | 493 | `BufFileDumpBuffer` :391 | MATCH |
| 14 | `BufFileReadCommon` | 592 | `BufFileReadCommon` :462 | MATCH |
| 15 | `BufFileRead` | 644 | `BufFileRead` :513 | MATCH |
| 16 | `BufFileReadExact` | 653 | `BufFileReadExact` :519 | MATCH |
| 17 | `BufFileReadMaybeEOF` | 663 | `BufFileReadMaybeEOF` :526 | MATCH |
| 18 | `BufFileWrite` | 675 | `BufFileWrite` :536 | MATCH |
| 19 | `BufFileFlush` | 719 | `BufFileFlush` :578 | MATCH |
| 20 | `BufFileSeek` | 739 | `BufFileSeek` :594 | MATCH |
| 21 | `BufFileTell` | 832 | `BufFileTell` :679 | MATCH |
| 22 | `BufFileSeekBlock` | 850 | `BufFileSeekBlock` :689 | MATCH |
| 23 | `BufFileSize` | 865 | `BufFileSize` :705 | MATCH |
| 24 | `BufFileAppend` | 901 | `BufFileAppend` :724 | MATCH |
| 25 | `BufFileTruncateFileSet` | 927 | `BufFileTruncateFileSet` :753 | MATCH |

## 2. Per-function notes (cross-checked logic)

- **Constants.** `MAX_PHYSICAL_FILESIZE = 0x40000000`, `BUFFILE_SEG_SIZE =
  MAX_PHYSICAL_FILESIZE / BLCKSZ`, `BLCKSZ = 8192` all match c2rust
  (lines 319, 320, 1259). Wait-event values verified against c2rust:
  `WAIT_EVENT_BUFFILE_READ = 167772166`, `_TRUNCATE = 167772167`, `_WRITE =
  167772168`, i.e. `PG_WAIT_IO(0x0A000000) | {6,7,8}` — exactly the port's
  `PG_WAIT_IO | {6,7,8}`. `O_RDONLY = 0`, `SEEK_{SET,CUR,END} = {0,1,2}`,
  `EOF = -1` all correct.
- **makeBufFileCommon/makeBufFile (1,2).** `numFiles` kept equal to
  `files.len()` (C tracks it as an explicit count; the canonical struct keeps
  both and the port syncs at every push/pop). All field inits match. The C
  `file->resowner = CurrentResourceOwner` is recorded as `None` (the fd/resowner
  edge is unported); only consumer is the `BufFileAppend` same-owner check —
  see (24).
- **extendBufFile (3).** Branch on `fileset == NULL`: temp file vs. new fileset
  segment, then push + bump count. C's `CurrentResourceOwner` save/restore is
  not modelled because fd.c owns owner association at open time (documented).
  `Assert(pfile >= 0)` → `debug_assert`. MATCH.
- **BufFileCreateTemp (4).** `PrepareTempTablespaces()` → tablespace seam;
  `OpenTemporaryFile` → fd seam; `makeBufFile` + set `isInterXact`. Allocates in
  `mcx` (C: current context). MATCH.
- **MakeNewFileSetSegment (6).** Unlink `segment+1` then create `segment`
  (crash-leftover guard), `Assert(file > 0)` → `debug_assert`. MATCH.
- **BufFileOpenFileSet (8).** C's manual capacity-doubling probe collapses to a
  growing `Vec` (behaviorally identical). Probe loop breaks on `f <= 0`,
  `CHECK_FOR_INTERRUPTS` each iteration, `nfiles == 0` → missing_ok/error with
  the last-probed `segment_name` (the `name.0` segment) and `name` — matches C
  error at line 336. `readOnly = (mode == O_RDONLY)`. MATCH.
- **BufFileDeleteFileSet (9).** Delete loop, `found`/`missing_ok` elog at 387.
  MATCH.
- **BufFileClose (11).** Flush + FileClose each segment; C's `pfree(files);
  pfree(file)` is the caller dropping the `PgBox` (the seam takes the box by
  value). MATCH.
- **BufFileLoadBuffer (12).** Component-advance predicate `curOffset >= MAX &&
  curFile+1 < numFiles`; `FileRead` into the full `BLCKSZ` buffer at
  `curOffset`; `nbytes < 0` → reset to 0 + file-access error (line 471);
  `temp_blks_read++` only when `nbytes > 0`. io-timing gated on
  `track_io_timing`. MATCH. (The fd seam returns `isize` and mirrors C's
  return-negative-on-error contract, so the `< 0` check is live.)
- **BufFileDumpBuffer (13).** While loop dumps the whole buffer across segment
  boundaries; extend-until-room (`while curFile+1 >= numFiles: extend`);
  `bytestowrite = min(nbytes - wpos, MAX - curOffset)`; `FileWrite`, `<= 0` →
  error (546); `temp_blks_written++` per write. Post-loop reconcile:
  `curOffset -= (nbytes - pos)`, segment-crossing fix `curOffset < 0`, then
  `pos = nbytes = 0`. All arithmetic widths verified (`availbytes`/`bytestowrite`
  fit usize since `curOffset ∈ [0, MAX)` after the advance and `nbytes ≤ BLCKSZ`).
  MATCH.
- **BufFileReadCommon (14).** Flush, then fill loop: reload when `pos >= nbytes`
  (advancing `curOffset += pos`), `nthistime = min(nbytes - pos, size)`, memcpy,
  advance. `exact` short-read predicate `nread != start_size && !(nread == 0 &&
  eofOK)` with the fileset-vs-temp message split (lines 632/634). MATCH. The
  read variants (15-17) delegate with the same `(exact, eofOK)` flags as C.
- **BufFileWrite (18).** `Assert(!readOnly)` → debug_assert; flush-or-reset on
  `pos >= BLCKSZ` (dirty → dump, else direct-from-read reset); `nthistime =
  min(BLCKSZ - pos, size)`; memcpy; `dirty = true`; grow `nbytes` to `pos`.
  MATCH.
- **BufFileSeek (20).** All three whence arms + default elog (779);
  `while newOffset < 0 { --newFile; if < 0 return EOF; += MAX }` (C `--newFile`
  pre-decrement preserved); in-buffer fast path; flush; "start of next seg"→"end
  of last seg" fixup; `while newOffset > MAX` forward walk with EOF; final EOF
  guard. SEEK_END size failure → shared `size_error` (774). MATCH.
- **BufFileSeekBlock (22).** `fileno = blknum / SEG_SIZE`, `offset = (blknum %
  SEG_SIZE) * BLCKSZ`, SEEK_SET. MATCH.
- **BufFileSize (23).** `(numFiles-1) * MAX + FileSize(last)`, error on negative
  (876). MATCH.
- **BufFileAppend (24).** `startBlock = numFiles * SEG_SIZE`;
  `Assert(readOnly)`, `Assert(!dirty)`; resowner-mismatch elog (912); splice
  source segments onto target. `source` consumed by value (C's ownership
  subsumption + caller-must-not-close). Owner check is `None == None` (resowner
  edge unported) — never the error, equivalent to the only supported same-owner
  case; documented. MATCH.
- **BufFileTruncateFileSet (25).** Descending loop `i = numFiles-1 .. fileno`;
  delete-vs-truncate branch predicate `(i != fileno || offset == 0) && i != 0`;
  delete path: FileClose + pop-tail + FileSetDelete (error 951) + `numFiles--`,
  `newOffset = MAX`, `if i == fileno: newFile--`; truncate path: FileTruncate
  `< 0` → error (969), `newOffset = offset`. Verified the `pop()` only ever
  removes the current tail: the truncate (else) branch fires only on the loop's
  final iteration (`i == 0` or `i == fileno`), so every prior iteration is a
  tail-delete — `debug_assert_eq!(files.len(), numFiles)` confirms. Post-loop
  three-way buffer-position reconciliation matches c2rust lines 1241-1257.
  MATCH.

## 3. Seam audit and wiring

**Owned seam crate (by C-source coverage of `buffile.c`):**
`backend-storage-file-buffile-seams`. It declares 6 seams
(`buf_file_create_temp`, `buf_file_close`, `buf_file_seek`, `buf_file_write`,
`buf_file_read_maybe_eof`, `buf_file_read_exact`) — the subset the cycle-partner
caller (nodeHashjoin, on main) needs. **All 6** are installed by this crate's
`init_seams()` (`src/seams.rs`), which contains nothing but `set()` marshal +
delegate closures. `seams-init::init_all()` calls
`backend_storage_file_buffile::init_seams()` (lib.rs:44). No uninstalled
declaration; no `set()` outside the owner. Seam type `nodehashjoin::BufFile`
re-exports `nodehash::BufFile` — same struct.

**Outward seam calls** (all justified by real unported-owner dependencies, all
thin marshal+delegate, no branching/computation in any seam path):

- `backend-storage-file-fd-seams` (fd.c, unported): `open_temporary_file`,
  `file_close`, `file_read`, `file_write`, `file_size`, `file_truncate`,
  `file_path_name`. Signatures mirror the C return contract (`isize`/`i64`
  negative-on-error preserved so the port's `< 0` checks are live).
- `backend-storage-file-fileset-seams` (fileset.c, unported): `file_set_create`,
  `file_set_open`, `file_set_delete`.
- `backend-commands-tablespace-seams` (tablespace.c): `prepare_temp_tablespaces`.
- `backend-tcop-postgres-seams` (postgres.c): `check_for_interrupts`
  (the `CHECK_FOR_INTERRUPTS` macro).

These declarations are owned/installed by their respective units (fd.c,
fileset.c, tablespace.c, postgres.c) — not by this `buffile.c` port — and panic
loudly until those land. No function body was replaced by a seam to "somewhere
else": every line of buffile.c logic lives in this crate; seams cross only true
external-owner boundaries.

**Direct (non-cycle) deps:** `backend_executor_instrument::with_pgBufferUsage`
(temp_blk counters), `backend_utils_misc_guc_tables::vars::track_io_timing`,
`portability_instr_time` (instr_time accum). Correct — no seam needed.

## 3b. Design conformance

- **No invented opacity (types.md 6-7).** `BufFile` is the real struct with all
  12 real fields; `FileSetHandle`/`File` are inherited opacity from their
  unported owners, not introduced here. No stand-in handles.
- **Allocating fns carry `Mcx` + `PgResult`.** `BufFileCreateTemp` /
  `BufFileCreateFileSet` / `BufFileOpenFileSet` take `Mcx<'mcx>` and return
  `PgResult<PgBox<'mcx, ...>>`. Compliant.
- **No shared statics for per-backend state.** None introduced; `track_io_timing`
  and `pgBufferUsage` are reached through their owners' thread-local accessors.
- **No ambient-global seams, no locks held across `?`, no registry side-tables,
  no unledgered divergence markers.** The two documented divergences (resowner
  recorded as `None` pending the fd/resowner edge; `size_error` hardcoding
  `EIO` at the seam boundary where the fd Ok-negative path carries no errno) are
  behavior-preserving for the supported cases and are noted in the crate docs.
- **Neighbor-dependency decisions** (AGENTS.md table): every unported neighbor
  (fd.c, fileset.c, tablespace.c, postgres.c) is handled by a per-owner seam
  crate that panics until the owner lands — the prescribed "seam-and-panic"
  path, not restructure-around or silent stub.

## 4. Build / test

- `cargo build -p backend-storage-file-buffile -p backend-storage-file-buffile-seams` — clean.
- `cargo test -p backend-storage-file-buffile` — 5 passed (seg-size formula,
  segment naming, wait-event values, makeBufFile init, Tell).

## Verdict

**PASS.** All 25 `buffile.c` functions MATCH. The single owned seam crate
(`backend-storage-file-buffile-seams`) has all 6 declarations installed by
`init_seams()` (wired into `init_all`); every outward seam is a justified,
thin marshal-and-delegate to a genuinely unported owner. Zero seam findings,
zero design-conformance findings.
