# Audit: backend-backup-manifest (backup_manifest.c)

C source: `src/backend/backup/backup_manifest.c` (397 LOC, PG 18.3).
Port: `crates/backend-backup-manifest/src/lib.rs`.

## Function inventory & verdicts

| # | C function (line) | Port | Verdict | Notes |
|---|---|---|---|---|
| 1 | `IsManifestEnabled` (33) static inline | `IsManifestEnabled` | MATCH | `buffile != NULL` → `buffile.is_some()`. |
| 2 | `AppendToManifest` (42) macro | `AppendStringToManifest` (caller-formatted) | MATCH | C macro = psprintf + AppendStringToManifest + pfree; port formats then calls AppendStringToManifest. The standalone `AppendToManifest` shim was correctly folded away (callers pass the formatted bytes directly). |
| 3 | `InitializeBackupManifest` (56) | `InitializeBackupManifest` | MATCH | memset→zeroed; checksum_type set; MANIFEST_OPTION_NO→buffile None; else BufFileCreateTemp(false)+cryptohash create/init (init<0 → elog ERROR); size=0; force_encode/first_file/still_checksumming set; header AppendToManifest with GetSystemIdentifier(). UINT64_FORMAT → decimal `{system_identifier}`. |
| 4 | `FreeBackupManifest` (91) | `FreeBackupManifest` | MATCH | pg_cryptohash_free + NULL. Guarded on non-null (C calls unconditionally; ctx is the palloc'd ptr — null-guard is behavior-equivalent since a disabled manifest never created one and free(NULL) for the opaque ctx is not exercised). |
| 5 | `AddFileToBackupManifest` (101) | `AddFileToBackupManifest` | MATCH | IsManifestEnabled early-return; tablespace path rewrite snprintf→snprintf_truncate(MAXPGPATH); first_file comma/newline; UTF-8 verify branch (Path+escape_json vs Encoded-Path+hex_encode); Size; Last-Modified GMT (pg_gmtime+pg_strftime, 128 buf); per-file checksum finalize+algorithm+hex when type!=NONE; close ` }`. |
| 6 | `AddWALInfoToBackupManifest` (212) | `AddWALInfoToBackupManifest` | MATCH | IsManifestEnabled early-return; terminate file list; readTimeLineHistory(endtli); WAL-Ranges open; per-entry skip-if-ended, first-range tli==endtli check, tl_beginptr (startptr vs entry.begin with invalid-begin guard), append range, break on starttli==tli else endptr=begin+first_wal_range=false; post-loop found_start_timeline check; terminate ranges. |
| 7 | `SendBackupManifest` (316) | `SendBackupManifest` | MATCH | IsManifestEnabled early-return; still_checksumming=false; cryptohash_final (<0 → elog ERROR); Manifest-Checksum prefix; hex_encode into string buf + NUL at LEN-1; append cstring (stop at NUL) + `"}\n`; BufFileSeek(0,0,SEEK_SET) nonzero→ereport file-access ERROR; bbsink_begin_manifest; while loop Min(buffer_length, remaining), BufFileReadExact into sink buffer slice, bbsink_manifest_contents, advance; bbsink_end_manifest; BufFileClose. |
| 8 | `AppendStringToManifest` (383) static | `AppendStringToManifest` | MATCH | len=strlen; if still_checksumming → cryptohash_update (<0 → elog ERROR); BufFileWrite; manifest_size += len. Assert(manifest!=NULL) is structural (&mut ref). |

Helpers added (not C functions, justified): `OidIsValid`, `XLogRecPtrIsInvalid`,
`lsn_format` (LSN_FORMAT_ARGS `%X/%X`), `sb_extend`/`sb_push`/`sb_grow`
(appendStringInfo/enlargeStringInfo over fallible mcx PgVec), `snprintf_truncate`
(snprintf "%s" into MAXPGPATH buffer), `elog_error`/`err_loc`.

## Constants verified against headers
- `SEEK_SET = 0` (stdio.h). MATCH.
- `MAXPGPATH = 1024` (types-core, pg_config_manual.h). MATCH.
- `PG_TBLSPC_DIR = "pg_tblspc"` (relpath.h). MATCH.
- `PG_UTF8 = 6` (pg_wchar.h). MATCH.
- `PG_SHA256_DIGEST_LENGTH = 32`, `PG_SHA256_DIGEST_STRING_LENGTH = 65`. MATCH.
- `PG_CHECKSUM_MAX_LENGTH = PG_SHA512_DIGEST_LENGTH = 64`. MATCH.
- Manifest version literal `2`, all JSON field names verified char-for-char
  against the C format strings (`PostgreSQL-Backup-Manifest-Version`,
  `System-Identifier`, `Files`, `Path`, `Encoded-Path`, `Size`,
  `Last-Modified`, `Checksum-Algorithm`, `Checksum`, `WAL-Ranges`, `Timeline`,
  `Start-LSN`, `End-LSN`, `Manifest-Checksum`). MATCH.
- strftime format `"%Y-%m-%d %H:%M:%S %Z"`. MATCH.

## Edge cases
- `BufFileSeek` returns `Ok(0)` success / `Ok(-1=EOF)` fail; port checks `!= 0`,
  matching C's `if (BufFileSeek(...))`. MATCH.
- hex_encode return value used as written-byte count; sb_grow over-reserves
  `2*len` then truncates to actual — matches C `buf.len += hex_encode(...)` over
  `enlargeStringInfo`. MATCH.
- pg_strftime count excludes the trailing NUL; truncate to `start+written`
  matches C `buf.len += pg_strftime(...)`. The impossible-overflow `None` maps to
  0 bytes written (the 128-byte buffer always fits the fixed-width timestamp).
- Min loop bound and `manifest_size` u64 vs usize compare: cast aligned. MATCH.

## Seam / wiring audit
- Owned seam crates: none. The unit's only C file is `backup_manifest.c`; there
  is no `crates/backup_manifest-seams` or `crates/backend-backup-manifest-seams`,
  and none is required — the sole C consumer is `basebackup.c` (catalog `todo`),
  which calls these functions directly and is acyclic with this crate, so it will
  depend on it directly when it lands. No `init_seams()` is needed (and none is
  declared), so there is no uninstalled-seam exposure.
- Outward calls: all are **direct deps** (no cycle): backend-backup-sink,
  backend-storage-file-buffile, backend-access-transam-timeline,
  backend-utils-adt-json, backend-utils-adt-encode, common-wchar,
  backend-timezone-localtime, backend-timezone-strftime, common-checksum-helper.
  Two genuine cross-cycle/primitive seams: `common-cryptohash-seams` (the
  external cryptographic primitive — correctly seamed, panics until its owner
  lands) and `backend-access-transam-xlog-seams::get_system_identifier` (xlog is
  a large cyclic owner; the seam is installed by xlog). Each seam call is thin
  marshal + delegate; no logic lives on a seam path.
- No function body was replaced by a "delegate elsewhere" seam; all manifest
  logic lives in this crate.

## Design conformance
- No invented opacity: `buffile` is the real `BufFile` (PgBox), `manifest_ctx`
  is the raw `*mut pg_cryptohash_ctx` C holds (typed C pointer, not a stand-in
  alias). No `type X = u64` handles.
- Allocation: the manifest byte stream uses fallible mcx `PgVec` via
  sb_* helpers (try_reserve → oom). `format!`/`lsn_format`/`snprintf_truncate`
  build small bounded intermediate strings (error messages at return-Err sites,
  or fixed-width LSN/Size tokens that are immediately copied into the fallible
  buffer) — within the allowed exceptions; none is on an unbounded data path.
- No shared statics for per-backend globals (manifest state is threaded by
  value); no ambient-global seams (ArchiveRecoveryRequested passed as the
  literal `false` per the timeline crate's documented base-backup convention,
  GetSystemIdentifier is a no-arg cluster constant, the blessed seam form).
- No locks held across `?`; BufFile is owned and closed explicitly (C `pfree`
  analog).
- One ledgered divergence (DESIGN_DEBT.md): the `pg_cryptohash_error(ctx)`
  detail suffix on the three checksum-failure messages is dropped because the
  seam does not expose it; SQLSTATE + primary message preserved. This is
  error-text only on paths unreachable under the in-tree SHA-256 fallback.

## Verdict: PASS

All 8 functions MATCH; zero seam findings; design conformance clean (one
ledgered error-text divergence). `cargo check --workspace`, `-p
backend-backup-manifest`, and `-p seams-init` are green; no `todo!`/`unimplemented!`.
