# Audit: backend-backup-sink-support (`basebackup_progress.c`)

Independent audit re-derived from `../pgrust/postgres-18.3/src/backend/backup/basebackup_progress.c`,
the c2rust rendering (`../pgrust/c2rust-runs/backend-backup-sink-support/src/basebackup_progress.rs`),
and `src/include/commands/progress.h`. Crate is a faithful port over the landed
`backend-backup-sink` `Bbsink<'mcx>` / `BbsinkOps` model.

## Function inventory

| C function (basebackup_progress.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `bbsink_progress_ops` static vtable (L42) | `impl BbsinkOps for BbsinkProgress` wiring | MATCH | begin_backup / archive_contents / end_archive overridden; the other 6 are pure `bbsink_forward_*`, matching the vtable field-for-field. |
| `bbsink_progress_new` (L58) | `bbsink_progress_new` | MATCH | `Assert(next != NULL)` → owned `Box<Bbsink>` (non-null by construction). `palloc0` → `Box::new(Bbsink::new(mcx, …))`. Calls `pgstat_progress_start_command(Basebackup, InvalidOid)` then `update_param(BACKUP_TOTAL, -1)`. `estimate_backup_size` accepted-but-unused, as in C. |
| `bbsink_progress_begin_backup` (L83) | `begin_backup` | MATCH | val[0]=PHASE_STREAM_BACKUP; val[1]=bytes_total if bytes_total_is_valid else -1; val[2]=list_length(tablespaces)=`tablespaces.len()`; `update_multi_param(3,…)`; then forward. |
| `bbsink_progress_end_archive` (L113) | `end_archive` | MATCH | guard `tablespace_num < list_length` → `update_param(TBLSPC_STREAMED, num+1)`; forward; then `tablespace_num += 1`. Statement ordering (report → forward → bump) preserved exactly. |
| `bbsink_progress_archive_contents` (L149) | `archive_contents` | MATCH | `bytes_done += len`; forward; nparam build: always STREAMED=bytes_done; additionally TOTAL=bytes_done iff `bytes_total_is_valid && bytes_done > bytes_total`; `update_multi_param(nparam,…)` via `&index[..nparam]`/`&val[..nparam]`. |
| `basebackup_progress_wait_checkpoint` (L185) | same | MATCH | `update_param(PHASE, WAIT_CHECKPOINT)`. |
| `basebackup_progress_estimate_backup_size` (L195) | same | MATCH | `update_param(PHASE, ESTIMATE_BACKUP_SIZE)`. |
| `basebackup_progress_wait_wal_archive` (L205) | same | MATCH | val[0]=WAIT_WAL_ARCHIVE; val[1]=list_length(tablespaces); `update_multi_param(2,…)`. |
| `basebackup_progress_transfer_wal` (L228) | same | MATCH | `update_param(PHASE, TRANSFER_WAL)`. |
| `basebackup_progress_done` (L238) | same | MATCH | `pgstat_progress_end_command()`. |
| `list_length` (inline; c2rust L604) | `state.tablespaces.len()` | MATCH | C returns 0 for NULL list; an empty/owned Vec yields 0 identically. |

## Constants (vs `commands/progress.h` L128–139)

PROGRESS_BASEBACKUP_PHASE=0, BACKUP_TOTAL=1, BACKUP_STREAMED=2, TBLSPC_TOTAL=3,
TBLSPC_STREAMED=4; PHASE_WAIT_CHECKPOINT=1, ESTIMATE_BACKUP_SIZE=2,
STREAM_BACKUP=3, WAIT_WAL_ARCHIVE=4, TRANSFER_WAL=5 — all transcribed exactly.
`ProgressCommandType::Basebackup`=5 == `PROGRESS_COMMAND_BASEBACKUP`; `InvalidOid`=0.

## Seam / wiring audit

- Owned seam crates: NONE. No `*-seams` crate maps to `basebackup_progress.c`;
  the file's only caller (`basebackup.c`) is a higher layer that will call these
  public free functions directly, exactly as the merged `backend-backup-sink`
  leaf is consumed. No inward seam is required (no dependency cycle).
- `init_seams()` is empty and registered in `seams-init::init_all` for
  uniformity; both recurrence guards pass (`every_seam_installing_crate…`,
  `every_declared_seam…`).
- `pgstat_progress_*` are reached as a direct, acyclic dependency on the ported
  owner `backend-utils-activity-small` — not a seam, and that owner is not part
  of this unit, so this crate carries no install obligation for them.

## Design conformance

- No invented opacity. No new fallible/allocating functions or seams (the lone
  allocation, `Bbsink::new`, takes the surrounding `Mcx` via the sink crate's
  existing API). No shared statics, ambient-global seams, locks-across-`?`, or
  registry side tables. No divergence markers needed.

## Verdict: PASS

Every function MATCH; zero seam findings; no design violations. 10 unit tests
pass (install the `backend_status` seams over a shared `PgBackendStatus` and
assert the exact `st_progress_*` writes). `cargo check --workspace`, the no-todo
guard, and `seams-init` recurrence guards are green.
