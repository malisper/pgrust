# Audit: backend-backup-server

C source: `src/backend/backup/basebackup_server.c` (310 lines).
Port: `crates/backend-backup-server/src/lib.rs`.
c2rust reference: `c2rust-runs/backend-backup-server/src/basebackup_server.rs`.

Re-derived independently from the C and c2rust; the port's comments were not
trusted.

## Function inventory & verdicts

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `bbsink_server_ops` table (44-54) | `impl BbsinkOps for BbsinkServer` | MATCH | begin_backup/end_backup/cleanup forward; the six middle ops call the server impls. |
| `bbsink_server_new` (59-128) | `bbsink_server_new` | MATCH | See detail below. |
| `bbsink_server_begin_archive` (133-154) | `begin_archive_impl` | MATCH | psprintf `"{path}/{name}"`; `PathNameOpenFile(O_CREAT\|O_EXCL\|O_WRONLY\|PG_BINARY)`; `file <= 0` → errcode_for_file_access "could not create file %m"; then forward. |
| `bbsink_server_archive_contents` (159-188) | `archive_contents_impl` | MATCH | `write_at`; advance filepos; forward. |
| `bbsink_server_end_archive` (193-217) | `end_archive_impl` | MATCH | FileSync (not data_sync_elevel); on err → "could not fsync file %m"; FileClose; file=0,filepos=0; forward. |
| `bbsink_server_begin_manifest` (227-247) | `begin_manifest_impl` | MATCH | psprintf `"{path}/backup_manifest.tmp"`; same open + error; forward. |
| `bbsink_server_manifest_contents` (252-281) | `manifest_contents_impl` | MATCH | identical to archive_contents over the .tmp file. |
| `bbsink_server_end_manifest` (286-309) | `end_manifest_impl` | MATCH | FileClose; file=0; durable_rename(tmp, final, ERROR); forward. |

### `bbsink_server_new` detail
- palloc0 struct → `BbsinkServer { pathname, file: 0, filepos: 0 }` boxed into
  `Bbsink::new(mcx, ops, Some(next))`. Field order preserved (pathname, file,
  filepos). MATCH.
- `StartTransactionCommand()` → `xact::start_transaction_command::call()?` (seam).
- `!has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES)` →
  `!acl::has_privs_of_role::call(miscinit::get_user_id::call(), ROLE_PG_WRITE_SERVER_FILES)?`.
  Error: ERRCODE_INSUFFICIENT_PRIVILEGE, errmsg + errdetail text verbatim. MATCH.
- `CommitTransactionCommand()` → seam call.
- `!is_absolute_path(pathname)` → `port_path_seams::is_absolute_path` (seam);
  ERRCODE_INVALID_NAME "relative path not allowed...". MATCH.
- `switch (pg_check_dir(pathname))`: `0` → MakePGDirectory, `<0` →
  errcode_for_file_access "could not create directory %m"; `1` → empty (noop);
  `2/3/4` → ERRCODE_DUPLICATE_FILE "directory %s exists but is not empty";
  `default` → errcode_for_file_access "could not access directory %m". Match
  arms `0 / 1 / 2|3|4 / _`. MATCH.

## Constants verified against C headers
- `ROLE_PG_WRITE_SERVER_FILES = 4570` — `pg_authid.dat` oid 4570 (verified).
- `WAIT_EVENT_BASEBACKUP_WRITE/SYNC` — `PG_WAIT_IO | {5,4}` from the 0-based IO
  index in `wait_event_names.txt` (BASEBACKUP_SYNC=4, BASEBACKUP_WRITE=5), the
  same derivation buffile.c uses (BUFFILE_READ = PG_WAIT_IO|6). Verified.
- `PG_WAIT_IO = 0x0A000000`. Verified vs buffile sibling.
- `OPEN_FLAGS = O_CREAT|O_EXCL|O_WRONLY` (PG_BINARY = 0 on POSIX). Verified vs
  c2rust.
- SQLSTATEs (INSUFFICIENT_PRIVILEGE 42501, INVALID_NAME 42602, DISK_FULL 53100,
  DUPLICATE_FILE 58P02) from `types-error`. Verified.

## Seam / wiring audit
- This unit owns one C file (`basebackup_server.c`). No other crate calls into
  it across a cycle, so it declares no inward seam crate; `init_seams()` is
  correctly empty. recurrence_guard (both directions) passes.
- Outward calls:
  - File IO (`PathNameOpenFile`, `FileWriteV`, `FileSync`, `FileClose`,
    `FilePathName`, `durable_rename`) — **direct dependency** on
    `backend-storage-file-fd` (no cycle: fd does not depend on any backup
    crate). Thin marshal only.
  - `make_pg_directory` — fd-seams seam (the fd impl is crate-private
    `seam_make_pg_directory`, exposed only via its installed seam).
  - `start/commit_transaction_command` (xact-seams), `has_privs_of_role`
    (acl-seams), `get_user_id` (miscinit-seams), `is_absolute_path`
    (port-path-seams), `pg_check_dir` (new common-pgcheckdir-seams). Each owner
    is unported or installs elsewhere; calls panic until the owner lands —
    mirror-PG-and-panic, no own logic deferred.
- New `common-pgcheckdir-seams` declares `pg_check_dir(dir)->i32` for the
  unported `common/pgcheckdir.c` owner; not installed by this crate (the owner
  installs it on landing). Decls-only, deps `seam-core`.

## Design conformance
- No invented opacity: `File` is the canonical `types_storage::File` VFD newtype;
  no stand-in aliases.
- Error paths return `Err(PgError)` (the repo BbsinkOps callbacks are fallible
  `PgResult<()>`), so no `panic!`/`unwrap` stands in for a C ereport. Zero
  `todo!`/`unimplemented!`.
- `%m` expansion driven by `with_saved_errno` + `errcode_for_file_access`,
  errno sourced from the fd `Err`'s `saved_errno()` or `errno::current_errno()`.
- The one `format!`/`clone` of `pathname` is the palloc0-struct field store
  (small, behavior-neutral); message `format!`s are at Err-return sites.

## Verdict: PASS
Every C function MATCH; zero MISSING/PARTIAL/DIVERGES; no seam findings.
`cargo check --workspace` clean; `cargo test -p seams-init` (recurrence guards)
green.

---

## Independent re-audit (2026-06-13, model claude-opus-4-8[1m])

Re-derived from scratch against `basebackup_server.c` (310 lines, 8 fns + the
`bbsink_server_ops` table) and the c2rust render — port comments not trusted.
Confirmed independently:
- All 8 C functions + the ops vtable present; every verdict above re-checked
  MATCH (control flow, error paths, the 0/1/2-3-4/default `pg_check_dir`
  dispatch, fsync-then-close vs close-then-`durable_rename`).
- Constants verified against C: `ROLE_PG_WRITE_SERVER_FILES = 4570`
  (`pg_authid.dat`); `WAIT_EVENT_BASEBACKUP_WRITE = 0x0A000005 = PG_WAIT_IO|5`
  and `WAIT_EVENT_BASEBACKUP_SYNC = 0x0A000004 = PG_WAIT_IO|4` (c2rust
  `167772165`/`167772164`); `OPEN_FLAGS = O_CREAT|O_EXCL|O_WRONLY`; errfinish
  line numbers (149/175/182/208/242 …) match c2rust.
- `FileWriteV` single-element iovec is behavior-equivalent to C `FileWrite`;
  Err(hard fail) and negative-Ok(FileAccess refusal) both map to C's
  `nbytes < 0` write-error branch; short-write → ERRCODE_DISK_FULL.
- Owns no inward seam crate → empty `init_seams()`, wired into `init_all()`.
  All 6 outward seam owners exist (xact/acl/miscinit/port-path/fd-seams) +
  the new `common-pgcheckdir-seams` decl; no own logic deferred across a seam.
- Gates: `cargo check --workspace` clean; `cargo test --workspace` 1558
  test-suite results all OK, zero FAILED (only the known timeout flakes); both
  `recurrence_guard` directions green.

Independent verdict: **PASS** (concurs with the original audit).
