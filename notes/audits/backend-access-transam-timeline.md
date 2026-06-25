# Audit: backend-access-transam-timeline

- **Verdict: PASS**
- Unit: `backend-access-transam-timeline` (`src/backend/access/transam/timeline.c`)
- Port: `crates/backend-access-transam-timeline`
- C source: `postgres-18.3/src/backend/access/transam/timeline.c` (592 lines)
- c2rust: `c2rust-runs/backend-access-transam-timeline/src/timeline.rs`
- Auditor: independent from-scratch re-derivation from C + c2rust (Claude Code,
  model Opus 4.8 / `claude-opus-4-8[1m]`)
- Date: 2026-06-13
- Re-audit of the prior FAIL (F1 below): the invented bundled `durable_write_file`
  fd seam has been removed and functions #5/#6 re-ported in-crate. Confirmed
  resolved.
- **Sync re-audit (merge of current `main`):** reconciled the shared-vocabulary
  collision in `backend-access-transam-timeline-seams`. `main` is authoritative
  on the live-consumer surface: the seam is renamed `readTimeLineHistory ->
  read_timeline_history(mcx, target_tli)` (main's 2-arg name+arity; consumers
  `backend-access-transam-xlogutils` and `backend-postmaster-walsummarizer` are
  unchanged), and the rest of the family is renamed onto main's spelling
  (`exists_timeline_history`, `find_newest_timeline`, `write_timeline_history`,
  `write_timeline_history_file`, `restore_timeline_history_files`). The branch's
  richer C-faithful shapes are kept where main only had unconsumed stubs:
  `exists_timeline_history` returns `PgResult<bool>` and takes `(mcx, probe_tli,
  archive_recovery_requested)` (C `ereport(FATAL)` on non-`ENOENT`), and
  `write_timeline_history_file(tli, &[u8])` — both match the live
  `backend-replication-walreceiver` call sites (call-site renames applied there).
  `TimeLineHistoryEntry`/`XLOGDIR` now resolve to `main`'s single owners
  (`types_wal::TimeLineHistoryEntry`, `types_wal::xlog_consts::XLOGDIR`); the
  branch-introduced duplicate `types_core::timeline` module was deleted. No
  function body was altered, so every per-function MATCH below stands; only the
  seam-name table in §3 was updated. `cargo check --workspace` + `cargo test
  --workspace` clean. Date: 2026-06-13, model `claude-opus-4-8[1m]`.

## 1. Function inventory and verdicts

Every function definition in timeline.c (plus the two macro-expanded inline
helpers c2rust kept). The c2rust rendering (`timeline.rs`) carries exactly these
nine `#[no_mangle]` functions plus `TLHistoryFileName`/`TLHistoryFilePath`/
`pgstat_report_wait_*`/`isspace`/`__istype` inline helpers — no function was
dropped from the inventory.

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `restoreTimeLineHistoryFiles` (50) | `lib.rs:99` | MATCH | `while tli < end`, `tli == 1` skip; `TLHistoryFileName`; `RestoreArchivedFile` (xlogarchive seam) called unconditionally (C does not gate this on `ArchiveRecoveryRequested`); `Some(path)` ⇒ `KeepFileRestoredFromArchive`. Loop bound + skip + increment match. |
| 2 | `readTimeLineHistory` (76) | `lib.rs:131` | MATCH | tli==1 ⇒ single tip entry (begin=end=Invalid). Archive branch driven by `archive_recovery_requested` param ⇒ `RestoreArchivedFile`, else `TLHistoryFilePath`. ENOENT ⇒ single tip entry; ferror ⇒ ERROR (raised inside the read seam). Parse loop: leading-whitespace + `#`/empty skip; `sscanf("%u\t%X/%X")`; nfields<1 ⇒ FATAL; nfields!=3 ⇒ FATAL; non-increasing tli ⇒ FATAL; entry `end=(hi<<32)|lo`, `begin=prevend`; `lcons` preserved by `insert(0,·)`; post-loop `targetTLI<=lasttli` ⇒ FATAL; tip entry prepended; `from_archive` ⇒ `KeepFileRestoredFromArchive`. List in caller `Mcx`. The parse core lives in-crate and is faithful. |
| 3 | `existsTimeLineHistory` (222) | `lib.rs:283` | MATCH | probeTLI==1 ⇒ false; archive vs pg_wal path; `file_exists` seam (open ok ⇒ true, ENOENT ⇒ false, other ⇒ FATAL). The probe is a single `AllocateFile`+`FreeFile`; bundling that pair as one generic `file_exists` primitive is acceptable. |
| 4 | `findNewestTimeLine` (264) | `lib.rs:309` | MATCH | `newestTLI=startTLI`; probe `+1,+2,…` until exists false; `wrapping_add` mirrors C unsigned increment. |
| 5 | `writeTimeLineHistory` (304) | `lib.rs:341` | **MATCH** | **(Re-port of the prior FAIL.)** The full emplacement orchestration now lives in-crate: temp-name `XLOGDIR "/xlogtemp.%d" (getpid)`, `unlink(tmppath)`, `OpenTransientFile(O_RDWR\|O_CREAT\|O_EXCL)` with the `fd<0` ereport, parent-path archive/pg_wal branch, `OpenTransientFile(O_RDONLY)` with the `srcfd<0`/ENOENT skip, the BLCKSZ `read`→`write` copy loop with `errno`-reset, the write-failure `save_errno`/`unlink`/`ENOSPC` handling, `CloseTransientFile(srcfd)`, the `(srcfd<0)?"":"\n"` leading-newline rule, the `"%s%u\t%X/%X\t%s\n"` line write with its own ENOSPC handling, `pg_fsync` at `data_sync_elevel(ERROR)`, `CloseTransientFile(fd)`, `TLHistoryFilePath`, the `access(F_OK)==ENOENT` assert (`debug_assert!`), `durable_rename(…, ERROR)`, and the `XLogArchivingActive()`-gated `XLogArchiveNotify`. Calls only the individual fd.c primitive seams (`open_transient_file`/`close_transient_file`/`pg_fsync`/`data_sync_elevel`/`durable_rename`) plus plain libc (`read`/`write`/`unlink`/`access`/`getpid`), exactly as timeline.c open-codes them and as the merged slru precedent does. `LSN_FORMAT_ARGS` ⇒ `(sp>>32) as u32`, `sp as u32` — order and width match. |
| 6 | `writeTimeLineHistoryFile` (463) | `lib.rs:537` | **MATCH** | **(Re-port of the prior FAIL.)** Body in-crate: temp-name, `unlink`, `OpenTransientFile O_EXCL` with `fd<0` ereport, single `write(content,size)` with `save_errno`/`unlink`/`ENOSPC` handling, `pg_fsync` at `data_sync_elevel(ERROR)`, `CloseTransientFile`, `TLHistoryFilePath`, `durable_rename(…, ERROR)` (replace-existing is implicit in `durable_rename`). Granular fd primitive seams only. |
| 7 | `tliInHistory` (526) | `lib.rs:610` | MATCH | Linear scan for `tle.tli == tli`. |
| 8 | `tliOfPointInHistory` (544) | `lib.rs:622` | MATCH | `(Invalid(begin)\|\|begin<=ptr) && (Invalid(end)\|\|ptr<end)` ⇒ tli; fallthrough ⇒ `elog(ERROR,…)` rendered as `errmsg_internal("timeline history was not contiguous")` — matches c2rust (`errmsg_internal`, elevel ERROR). |
| 9 | `tliSwitchPoint` (572) | `lib.rs:648` | MATCH | `nextTLI` out-param returned as tuple `.1`; match returns `(tle.end, nextTLI)` BEFORE the `nextTLI = tle.tli` update, so a first-entry match yields nextTLI=0 — matches C ordering exactly. Not found ⇒ ERROR "requested timeline %u is not in this server's history". |

Inline helpers `TLHistoryFileName`/`TLHistoryFilePath` (macros from
`access/xlog_internal.h`) → `lib.rs:80/86`, formats `"%08X.history"` /
`XLOGDIR "/%08X.history"` — MATCH.

## 2. Pure-parsing fidelity (readTimeLineHistory inner loop)

The in-crate parsing path reproduces the C `fgets`+`isspace`/`#`+`sscanf` loop
1:1 and is correct:
- `history_file_lines`: fgets chunking (line up to and incl `\n`, or 1023
  bytes, whichever first; trailing chunk without `\n`).
- `chunk_to_cstr`: C string ops stop at the first embedded NUL.
- `is_c_space`: the "C"-locale `isspace` set (` \t\n\r\x0b\x0c`).
- `sscanf_history_line`: `%u` decimal (glibc optional sign, 32-bit wrapping),
  format whitespace matches any (possibly empty) run, literal `/` must match,
  `%X` hex (optional sign + `0x` prefix, 32-bit wrapping); conversion count
  0..=3 returned. Verified against glibc scanf semantics.

28 crate unit tests pass, covering tli==1, missing-file, parse field counts,
tab-as-whitespace, lookups, switch-point ordering, and the write-path parent
copy / leading-newline / archive-notify behaviors.

## 3. Seam audit (PASS)

Owned seam crate (by C-source coverage, timeline.c ⇒
`backend-access-transam-timeline-seams`): all nine declarations
(`read_timeline_history`, `exists_timeline_history`, `find_newest_timeline`,
`write_timeline_history`, `write_timeline_history_file`,
`restore_timeline_history_files`, `tli_in_history`, `tli_of_point_in_history`,
`tli_switch_point` — main's authoritative spelling after the sync) are installed
by this crate's `init_seams()`,
which is `set()`-only, and `crates/seams-init/src/lib.rs:14` calls
`backend_access_transam_timeline::init_seams()`. Inward wiring: clean.

Outward seams — all justified thin marshal+delegate, no relocated logic:

- **fd.c primitives** (`backend-storage-file-seams`): `open_transient_file`,
  `close_transient_file`, `pg_fsync`, `data_sync_elevel`, `durable_rename` —
  genuine fd.c functions, each a one-call delegate. The orchestration that wires
  them stays in the two write functions, in-crate.
- **`read_file_or_absent`** (`backend-storage-file-fd-seams`): a generic
  "`AllocateFile("r")`+read-loop+`FreeFile`, None on ENOENT" primitive, used
  only by `readTimeLineHistory`; the timeline-specific parsing stays in-crate.
  Acceptable generic capability. (The prior F2 concern — that this bundled read
  was reused on the *write* parent-copy path — is moot: `writeTimeLineHistory`
  now uses a real streamed `OpenTransientFile`+`read` BLCKSZ loop, byte- and
  failure-granularity-identical to C.)
- **`file_exists`** (`backend-storage-file-fd-seams`): the
  `AllocateFile`+`FreeFile` existence probe of `existsTimeLineHistory`.
- **xlogarchive** (`backend-access-transam-xlogarchive-seams`):
  `restore_archived_history_file`, `keep_file_restored_from_archive`,
  `xlog_archive_notify` — thin delegates to genuine xlogarchive.c functions.

### Prior FAIL (F1) — resolved

The previous audit failed on an invented bundled fd seam
`durable_write_file(final_path, content, replace_existing)` that relocated
timeline.c's own temp-file emplacement orchestration out of functions #5/#6.
Verified resolved: `grep -rn durable_write_file crates/` returns nothing; the
seam declaration is gone; #5 and #6 carry the full orchestration in-crate over
granular fd primitives. Re-derived #5/#6 from scratch against the C — MATCH.

## 4. Design conformance (§3b)

- No file-scope `static`/`Atomic`/`Mutex` (timeline.c has none).
  `ArchiveRecoveryRequested`/`XLogArchivingActive()` are foreign per-backend
  globals passed as explicit `bool` params (AGENTS.md "per-backend global's
  value → explicit parameter") — no ambient-getter seam. `xlog_archiving_active`
  mirrors the macro `XLogArchiveMode > ARCHIVE_MODE_OFF`. Conformant.
- No invented opacity / integer-alias stand-ins; `TimeLineHistoryEntry` and
  `XLOGDIR` are real (`access/timeline.h`, `access/xlog_internal.h`). Conformant.
- Allocations fallible (`vec_with_capacity_in`/`try_reserve` ⇒ `mcx.oom`); the
  one palloc'd output (the entry list) is built in the caller's `Mcx`. Owned
  FATAL/ERROR ereports are `Err(PgResult)` carrying matching SQLSTATE/severity
  (`errcode_for_file_access`, `with_saved_errno` for `%m`). Conformant.
- No bundled seam relocating crate-owned logic (the F1 violation is gone).
  Conformant.

## 5. Verdict

**PASS.** All nine functions MATCH; the parsing and in-memory-lookup logic is
faithful; seam wiring is clean (inward installed, outward thin delegates over
genuine owners); design rules satisfied. The prior FAIL finding F1 (invented
`durable_write_file` bundled seam) is resolved, with #5/#6 re-derived from C and
re-verified MATCH. Crate may merge; mark the `CATALOG.tsv` row `audited`.
