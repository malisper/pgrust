# Audit: backend-libpq-be-secure-common

C source: `src/backend/libpq/be-secure-common.c` (SSL-library-independent
secure-transport helpers). Crate: `crates/backend-libpq-be-secure-common`.

Audit re-derived from the C source, c2rust rendering
(`c2rust-runs/backend-libpq-be-secure-common/src/be_secure_common.rs`), and the
POSIX `<sys/stat.h>` mode constants. Independent of the port's comments.

## Function inventory

The C file defines exactly two functions (no statics/inlines; confirmed against
c2rust, which kept both and only the externed libc/elog decls).

| C fn | C loc | Port loc | Verdict | Notes |
|------|-------|----------|---------|-------|
| `run_ssl_passphrase_command` | be-secure-common.c:39-107 | lib.rs:70-167 | MATCH | see below |
| `check_ssl_key_file_permissions` | be-secure-common.c:113-177 | lib.rs:177-238 | MATCH | see below |

### run_ssl_passphrase_command — MATCH

- `loglevel = is_server_start ? ERROR : LOG` — matches (lib.rs:80).
- `Assert(size > 0)` -> `debug_assert!` (lib.rs:83). `Assert(prompt)` is a
  non-null assert on a `&str`, vacuous in Rust.
- `buf[0] = '\0'` -> empty `PgVec` accumulator; the result is built up and
  returned, with length == the C return value.
- `replace_percent_placeholders(ssl_passphrase_command, "ssl_passphrase_command",
  "p", prompt)` — matches: GUC read via guc-tables `vars`, `[('p', Some(prompt))]`,
  `?` propagates the bad-template `ereport(ERROR)` unconditionally as C does.
  NULL/empty GUC -> empty template (`unwrap_or_default`), which the C passes
  through unchanged (default is `""`).
- `OpenPipeStream(command, "r")` NULL -> `ereport(loglevel,
  errcode_for_file_access, "could not execute command \"%s\": %m"); goto error`
  — matches (lib.rs:101-114). Backed by the new fd seam `open_pipe_stream_read`,
  whose owner (`OpenPipeStreamOrNull`) faithfully returns NULL+errno on a popen
  failure rather than ereport'ing (C contract); errno read via `last_errno`.
- `fgets`/`ferror`: `Line` writes up to `size-1` bytes into buf; `Eof` (NULL,
  !ferror) falls through; `Error` (ferror) -> `explicit_bzero(buf)` THEN
  `ereport(loglevel, errcode_for_file_access, "could not read from command...%m");
  goto error` — order and severity match (lib.rs:117-135).
- `ClosePipeStream`: `-1` -> bzero + "could not close pipe to external command:
  %m"; `!= 0` -> bzero + `reason = wait_result_to_str(rc)` +
  "command \"%s\" failed" + `errdetail_internal("%s", reason)` —
  matches (lib.rs:138-160). `wait_result_to_str` via unported owner's seam.
- `len = pg_strip_crlf(buf)` -> `strip_crlf_in_place` truncates to the stripped
  length (lib.rs:163). Return value == final buf length.

Message strings verified verbatim against C lines 59/71/83/94/96.

Minor note (not a divergence; consistent with repo precedent): the `pclose ==
-1` `%m` errno depends on the OS errno being set by `pclose`; the fd port's
`pclose` returns -1 from `child.wait()` without guaranteeing errno, same as the
existing `close_pipe_to_program` consumer. Behaviour-equivalent for the SQLSTATE
selection.

### check_ssl_key_file_permissions — MATCH

- `loglevel = isServerStart ? FATAL : LOG` — matches (lib.rs:185).
- `stat(ssl_key_file, &buf) != 0` -> `ereport(loglevel, errcode_for_file_access,
  "could not access private key file \"%s\": %m"); return false` — matches
  (lib.rs:188-200). `stat` is a direct POSIX syscall (no PG TU owns it; matches
  xlogarchive.c / miscinit.c precedent), returning `(st_mode, st_uid)` or errno.
- `!S_ISREG(buf.st_mode)` -> `ERRCODE_CONFIG_FILE_ERROR` "is not a regular file";
  return false — matches (lib.rs:203-210). `S_IFMT=0o170000`, `S_IFREG=0o100000`
  verified.
- `buf.st_uid != geteuid() && buf.st_uid != 0` -> `ERRCODE_CONFIG_FILE_ERROR`
  "must be owned by the database user or root"; return false — matches
  (lib.rs:215-221). `geteuid()` direct syscall.
- `(st_uid==euid && mode&(S_IRWXG|S_IRWXO)) || (st_uid==0 &&
  mode&(S_IWGRP|S_IXGRP|S_IRWXO))` -> `ERRCODE_CONFIG_FILE_ERROR`
  "has group or world access" + the u=rw/0600,0640 errdetail; return false —
  matches (lib.rs:223-235). Constants verified:
  `S_IRWXG=0o70, S_IRWXO=0o7, S_IWGRP=0o20, S_IXGRP=0o10`.
- `return true` — matches (lib.rs:238). The `#if !WIN32 && !__CYGWIN__` guard is
  the build config (always taken on this target; c2rust ran post-preprocessor).

errdetail string verified verbatim against C line 171.

## Seam audit

Owned seam crate: `backend-libpq-be-secure-common-seams` (maps to
be-secure-common.c). Both declarations
(`run_ssl_passphrase_command`, `check_ssl_key_file_permissions`) are installed by
`backend_libpq_be_secure_common::init_seams()`, which contains only `set()`
calls and is wired into `seams-init::init_all()`. PASS.

Outward seam calls — all thin marshal+delegate, justified:
- `backend_storage_file_fd_seams::{open_pipe_stream_read, pipe_read_line,
  close_pipe_stream, last_errno}` — fd.c-owned pipe I/O. New seams added to the
  fd owner this change; installed by fd's `init_seams()`. The adapters in
  `fd/src/seams.rs` are pure type-marshal (enum remap, token wrap); the real
  fgets byte-loop and NULL-on-popen logic live in the fd owner
  (`allocated_desc::{OpenPipeStreamOrNull, pipe_read_line}`), which is correct —
  that is fd.c's logic, not be-secure-common's.
- `common_wait_error_seams::wait_result_to_str::call` — owner unported;
  seam-and-panic, correct.

Direct (non-seam) deps: guc-tables `vars` (GUC read), common-percentrepl,
common-string, libc (stat/geteuid syscalls) — all acyclic.

## Design conformance

- `run_ssl_passphrase_command` seam is `Mcx` + `PgResult<PgVec>` (allocating,
  fallible) — conforms. `check_ssl_key_file_permissions` is non-allocating bool,
  no Mcx — conforms.
- No invented opacity (`PgFileStream` is the existing fd token; `PipeReadLine`
  enum carries owned bytes/errno).
- No shared statics for per-backend globals; `format!`/`String::from_utf8_lossy`
  only at error-report / strip-crlf sites; `unwrap_or` only on infallible
  fallbacks (errno=0, empty GUC template).
- No locks across `?`; no registry side tables.

## Verdict: PASS

Both functions MATCH; zero seam findings; design-conformant.
