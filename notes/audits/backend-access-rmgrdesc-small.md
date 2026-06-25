# Audit: backend-access-rmgrdesc-small

Unit: `backend-rmgrdesc-small` — the nine small rmgr descriptor files under
`src/backend/access/rmgrdesc/` (PostgreSQL 18.3): `clogdesc.c` (59),
`committsdesc.c` (54), `dbasedesc.c` (75), `genericdesc.c` (55),
`logicalmsgdesc.c` (52), `relmapdesc.c` (47), `rmgrdesc_utils.c` (61),
`seqdesc.c` (46), `tblspcdesc.c` (56).
Crate audited: `crates/backend-access-rmgrdesc-small`.
Cross-checked against `../pgrust/c2rust-runs/backend-rmgrdesc-small/src/*.rs`
(20 function definitions there — matches the C inventory exactly; no
preprocessor-conditional code in any of these files).
Auditor: independent re-derivation from the C sources and headers
(`access/clog.h`, `access/commit_ts.h`, `commands/dbcommands_xlog.h`,
`replication/message.h`, `utils/relmapper.h`, `commands/sequence.h`,
`storage/relfilelocator.h`, `commands/tablespace.h`, `access/xlogrecord.h`,
`storage/off.h`).

## Signature convention (applies to every `*_desc`)

C `void f(StringInfo buf, XLogReaderState *record)` becomes
`fn f(buf: &mut PgString<'_>, info: u8, data: &[u8]) -> PgResult<()>` where
`info` = `XLogRecGetInfo(record)` (each function masks `& !XLR_INFO_MASK`
exactly where the C does) and `data` = `XLogRecGetData(record)` with
`data.len()` = `XLogRecGetDataLen(record)`. Struct-cast/`memcpy` field reads
become bounds-checked native-endian reads at the `#[repr(C)]` offsets
(re-derived below per struct). A payload too short for a read raises
`ERRCODE_DATA_CORRUPTED` — in C that case reads adjacent garbage memory
(impossible for well-formed WAL), so behavior is identical on every input the
C handles defined-ly. `appendStringInfo` OOM (`ereport(ERROR)`) maps to the
fallible `try_push_*`/`append_fmt` returning the context's OOM `PgError`.
`*_identify` returns `Option<&'static str>` for `const char *`/NULL.

## Constants verified against headers

| Constant | Header | Value | Port |
|---|---|---|---|
| `XLR_INFO_MASK` | xlogrecord.h:62 | 0x0F | `types-wal::XLR_INFO_MASK` = 0x0F ✓ |
| `CLOG_ZEROPAGE` / `CLOG_TRUNCATE` | clog.h:55-56 | 0x00 / 0x10 | ✓ |
| `COMMIT_TS_ZEROPAGE` / `COMMIT_TS_TRUNCATE` | commit_ts.h:46-47 | 0x00 / 0x10 | ✓ |
| `XLOG_DBASE_CREATE_FILE_COPY` / `_CREATE_WAL_LOG` / `_DROP` | dbcommands_xlog.h:21-23 | 0x00 / 0x10 / 0x20 | ✓ |
| `XLOG_LOGICAL_MESSAGE` | message.h:37 | 0x00 | ✓ |
| `XLOG_RELMAP_UPDATE` | relmapper.h:25 | 0x00 | ✓ |
| `XLOG_SEQ_LOG` | sequence.h:46 | 0x00 | ✓ |
| `XLOG_TBLSPC_CREATE` / `XLOG_TBLSPC_DROP` | tablespace.h:27-28 | 0x00 / 0x10 | ✓ |

Struct offsets re-derived (LP64, `#[repr(C)]`): `xl_clog_truncate`
{pageno i64@0, oldestXact u32@8, oldestXactDb u32@12}; `xl_commit_ts_truncate`
{pageno i64@0, oldestXid u32@8}; `xl_dbase_create_file_copy_rec` {db_id@0,
tablespace_id@4, src_db_id@8, src_tablespace_id@12}; `xl_dbase_create_wal_log_rec`
{db_id@0, tablespace_id@4}; `xl_dbase_drop_rec` {db_id@0, ntablespaces i32@4,
tablespace_ids[]@8}; `xl_logical_message` {dbId@0, transactional bool@4,
pad→8, prefix_size usize@8, message_size usize@16, message[]@24};
`xl_relmap_update` {dbid@0, tsid@4, nbytes i32@8}; `xl_seq_rec.locator`
{spcOid@0, dbOid@4, relNumber@8}; `xl_tblspc_create_rec` {ts_id@0,
ts_path[]@4}; `xl_tblspc_drop_rec` {ts_id@0}. All port offsets match.

## Function inventory (all 20 definitions)

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `clog_desc` (clogdesc.c:20) | `clogdesc.rs::clog_desc` | MATCH | Masked info; ZEROPAGE: i64@0, `"page %" PRId64` → `"page {pageno}"`; TRUNCATE: pageno i64@0 + oldestXact u32@8, `"page %ld; oldestXact %u"`. C memcpys the whole 16-byte struct but prints only the first two fields — port reads only the printed 12 bytes; differs only when the payload is 12-15 bytes, where the C reads garbage (UB). Unknown info: silent no-op, as C. |
| 2 | `clog_identify` (clogdesc.c:42) | `clogdesc.rs::clog_identify` | MATCH | Masked switch; ZEROPAGE/TRUNCATE/"None" exactly. |
| 3 | `commit_ts_desc` (committsdesc.c:20) | `committsdesc.rs::commit_ts_desc` | MATCH | ZEROPAGE: bare `"%" PRId64` (no "page" prefix — preserved); TRUNCATE: `"pageno %ld, oldestXid %u"` from i64@0/u32@8. |
| 4 | `commit_ts_identify` (committsdesc.c:42) | `committsdesc.rs::commit_ts_identify` | MATCH | C switches on the **raw** info byte without masking (unlike siblings); port deliberately matches raw, with a comment. Verified against C and c2rust. |
| 5 | `dbase_desc` (dbasedesc.c:21) | `dbasedesc.rs::dbase_desc` | MATCH | FILE_COPY prints `"copy dir %u/%u to %u/%u"` with (src_tablespace_id, src_db_id, tablespace_id, db_id) — argument order vs struct order re-derived and correct in port. WAL_LOG: `"create dir %u/%u"` (tablespace_id, db_id). DROP: `"dir"` then `" %u/%u"` per tablespace; `for i in 0..ntablespaces` over i32 — negative count yields zero iterations, same as the C `for`. |
| 6 | `dbase_identify` (dbasedesc.c:58) | `dbasedesc.rs::dbase_identify` | MATCH | Masked; three subtype names + None. |
| 7 | `generic_desc` (genericdesc.c:23) | `genericdesc.rs::generic_desc` | MATCH | No info masking in C (record has no subtypes); port takes data only. Cursor walk: read u16 offset, u16 length, skip `length` bytes; trailing-vs-separator format chosen by `ptr < end` after the skip — identical. `%u` of u16 == u16 Display. Region header straddling the end errors instead of reading past (C UB). |
| 8 | `generic_identify` (genericdesc.c:52) | `genericdesc.rs::generic_identify` | MATCH | Unconditionally `"Generic"`. |
| 9 | `logicalmsg_desc` (logicalmsgdesc.c:18) | `logicalmsgdesc.rs::logicalmsg_desc` | MATCH | Masked; transactional bool@4 (C truthiness: `!= 0`), prefix_size/message_size as native `Size`. `%s` of the prefix stops at the first NUL — port scans for NUL within `prefix_size` bytes (the C `Assert(prefix[prefix_size-1] == '\0')` guarantees one is there; mirrored as `debug_assert`). Payload at `message + prefix_size`, hex-dumped `"%s%02X"` with `""`/`" "` separator state — loop identical (`{:02X}` of u8 == `%02X` of unsigned char). Non-UTF-8 prefix rendered lossily — C would pass raw bytes through; only differs on non-UTF-8 prefixes, which `LogLogicalMessage` callers supply as C strings. Unknown info: no-op. |
| 10 | `logicalmsg_identify` (logicalmsgdesc.c:46) | `logicalmsgdesc.rs::logicalmsg_identify` | MATCH | Masked compare → "MESSAGE" / None. |
| 11 | `relmap_desc` (relmapdesc.c:19) | `relmapdesc.rs::relmap_desc` | MATCH | `"database %u tablespace %u size %d"` — nbytes read as i32 and printed signed (`%d`), dbid/tsid as u32. |
| 12 | `relmap_identify` (relmapdesc.c:34) | `relmapdesc.rs::relmap_identify` | MATCH | Masked; "UPDATE" / None. |
| 13 | `array_desc` (rmgrdesc_utils.c:23) | `rmgrdesc_utils.rs::array_desc` | MATCH (restructured) | C's type-erased `(void *array, size_t elem_size, int count, fn, void *data)` becomes `&[T]` + `FnMut(&mut PgString, &T)` closure (captures replace `data`). Empty → `" []"` early return; else `" ["`, elements `", "`-separated, `']'` — exact. C with negative count produces `" ["` + `"]"` = `" []"`, same visible output; a slice cannot express negative counts. Provably identical for every representable input. |
| 14 | `offset_elem_desc` (rmgrdesc_utils.c:43) | `rmgrdesc_utils.rs::offset_elem_desc` | MATCH | `%u` of one `OffsetNumber` (u16). Unused `data` param subsumed by closure form. |
| 15 | `redirect_elem_desc` (rmgrdesc_utils.c:49) | `rmgrdesc_utils.rs::redirect_elem_desc` | MATCH | C reads `new_offset[0]`/`new_offset[1]` from a pair of adjacent OffsetNumbers; port takes `&[OffsetNumber; 2]`, `"%u->%u"`. |
| 16 | `oid_elem_desc` (rmgrdesc_utils.c:58) | `rmgrdesc_utils.rs::oid_elem_desc` | MATCH | `%u` of one `Oid` (u32). |
| 17 | `seq_desc` (seqdesc.c:21) | `seqdesc.rs::seq_desc` | MATCH | C casts `xlrec` before the info check but only dereferences inside the branch; port reads inside the branch — equivalent. `"rel %u/%u/%u"` of spcOid/dbOid/relNumber. |
| 18 | `seq_identify` (seqdesc.c:35) | `seqdesc.rs::seq_identify` | MATCH | Masked; "LOG" / None. |
| 19 | `tblspc_desc` (tblspcdesc.c:21) | `tblspcdesc.rs::tblspc_desc` | MATCH | CREATE: `"%u \"%s\""` — ts_id u32@0, ts_path = NUL-terminated bytes from @4 (`%s` semantics: stop at NUL); a payload with no NUL errors instead of reading past the record (C UB). DROP: `"%u"`. |
| 20 | `tblspc_identify` (tblspcdesc.c:38) | `tblspcdesc.rs::tblspc_identify` | MATCH | Masked; CREATE/DROP/None. |

Helper module `util.rs` (no C counterpart; `appendStringInfo` +
bounds-checked-read plumbing) audited: `append_fmt` surfaces the underlying
`PgError` from `try_push_str` (the `palloc`-ERROR path) rather than inventing
one; `bytes_at`/`read_*` are offset+width slices with `checked_add` overflow
guard; `read_size` reads a native `size_t`. No logic beyond marshalling.

## Spot-check re-derivations (anti-skim)

- **#5 dbase_desc FILE_COPY argument order**: struct fields are declared
  db_id, tablespace_id, src_db_id, src_tablespace_id (dbcommands_xlog.h:31-34)
  but the format consumes src_tablespace_id, src_db_id, tablespace_id, db_id —
  i.e. offsets 12, 8, 4, 0. The port reads each field at its declared offset
  and interpolates `{src_tablespace_id}/{src_db_id} to {tablespace_id}/{db_id}`
  — byte-for-byte the C output.
- **#9 logicalmsg offsets**: `Oid dbId` (4) + `bool transactional` (1 @4) +
  3 pad + `Size prefix_size` @8 + `Size message_size` @16 + `message` @24 on
  LP64. Port: `TRANSACTIONAL_OFF=4`, `PREFIX_SIZE_OFF=8`,
  `MESSAGE_SIZE_OFF=8+size_of::<usize>()`, `MESSAGE_OFF=+usize` — correct and
  correct on 32-bit too (4/8/12/16 there, matching that ABI's layout).
- **#7 generic_desc separator**: C appends `"; "` when, after consuming the
  region's data, more bytes remain (`ptr < end`), else no separator; port
  compares the same advanced cursor against `data.len()`. A length field that
  overruns the end makes `ptr >= end` in both, terminating with the
  no-separator form.
- **#4 commit_ts_identify no-mask**: confirmed in both the C
  (`switch (info)`) and the c2rust rendering; the port's raw match means e.g.
  `info = 0x01` returns None in both (and would in the masked variant return
  Some — the port did not "fix" the C's quirk).

## Seam audit

- The crate makes **zero** outward seam calls: every function bottoms out in
  `PgString` appends and slice reads. Correct — these are leaf routines.
- No `<unit>-seams` crate exists for this unit and none is declared anywhere;
  nothing to install. `init_seams()` is empty and is still called from
  `seams-init::init_all()` (`crates/seams-init/src/lib.rs:11`), per convention.
- No `set()` calls outside an owner; no logic hidden in seam paths.
- Dependencies are types-only (`mcx`, `types-core`, `types-error`,
  `types-wal`); `XLR_INFO_MASK` lives in `types-wal` and matches the header.

## Build / tests

`cargo build --workspace` clean; `cargo test -p backend-access-rmgrdesc-small`:
9 tests pass (cover each desc/identify family plus `array_desc`).

## Verdict

**PASS** — all 20 functions MATCH; no MISSING/PARTIAL/DIVERGES; no seam
findings. Divergences exist only on inputs where the C reads out of bounds
(undefined behavior), where the port raises `ERRCODE_DATA_CORRUPTED` instead.
