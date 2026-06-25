# Audit: backend-commands-copyfromparse

- **Unit:** `backend-commands-copyfromparse`
- **C source:** `src/backend/commands/copyfromparse.c` (PostgreSQL 18.3, 2059 lines)
- **Branch:** `port/backend-commands-copyfromparse`
- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

## Top-line verdict: **PASS**

Every C function is `MATCH` or properly `SEAMED`. Zero seam findings, zero
design-conformance findings. Logic parity verified function-by-function against
the C and the c2rust rendering
(`../pgrust/c2rust-runs/backend-commands-copyfromparse/src/copyfromparse.rs`).

## 1. Function inventory + verdicts

The c2rust run kept all 23 top-level definitions plus header-inlined helpers
(`list_length`, `list_nth_cell`, `list_nth_int`, `TupleDescAttr`, `ExecEvalExpr`,
`pq_writeint8/16`, `pq_sendint8/16/byte`) which belong to other files and are
seamed, not part of this unit. The two macros (`IF_NEED_REFILL_*`,
`REFILL_LINEBUF`) and `BinarySignature` are also rows.

| # | C function (line) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ReceiveCopyBegin` (169) | `ReceiveCopyBegin` 117 | SEAMED | Body is entirely libpq frontend (`pq_beginmessage`/`pq_sendbyte`/`pq_sendint16`×natts/`pq_endmessage`, set `copy_src=COPY_FRONTEND`, `fe_msgbuf=makeStringInfo`, `pq_flush`). Port computes `natts`/`binary` and delegates the whole message build to `receive_copy_begin` seam (libpq, unported). The per-column loop is intrinsic to the CopyInResponse wire format and lives with the pq_* calls — genuine cross-subsystem delegation, not displaced parser logic. |
| 2 | `ReceiveCopyBinaryHeader` (189) | 127 | MATCH | 11-byte signature compare against `BINARY_SIGNATURE`, flags `(1<<16)` WITH-OIDS check, `tmp &= ~(1<<16)`, `(tmp>>16)` critical-flags check, length `<0` check, extension skip loop. All SQLSTATEs `BAD_COPY_FILE_FORMAT`. `while(tmp-- > 0)` rendered as `while tmp>0 { tmp-=1; ... }` — identical iteration count. |
| 3 | `CopyGetData` (244) | `CopyGetData` 188 | SEAMED | The three source legs (FILE `fread`+`ferror`; FRONTEND `pq_getmessage`/message-type dispatch loop; CALLBACK) are libpq/file I/O across `copy_get_data_{file,frontend,callback}` seams. The `raw_reached_eof` bookkeeping the C does inline (FILE bytesread==0; FRONTEND CopyDone) is reported back via `reached_eof` and folded in-crate. No parser logic displaced. |
| 4 | `CopyGetInt32` (361) | `CopyGetInt32` 214 | MATCH | `pg_ntoh32` ⇒ `i32::from_be_bytes`; EOF (read != 4) ⇒ `*val=0; false`. |
| 5 | `CopyGetInt16` (378) | `CopyGetInt16` 227 | MATCH | `pg_ntoh16` ⇒ `i16::from_be_bytes`; EOF ⇒ `*val=0; false`. |
| 6 | `CopyConvertBuf` (399) | `CopyConvertBuf` 244 | MATCH | Both branches reproduced. Non-transcoding: `preverifiedlen`/`unverifiedlen`, `pg_encoding_verifymbstr`, `nverified==0` ⇒ EOF/`max_length` error predicate. Transcoding: `RAW_BUF_BYTES==0` EOF, `memmove` down ⇒ `copy_within`, `input_buf[nbytes]='\0'`, `pg_do_encoding_conversion_buf`, `convertedlen==0` ⇒ `MAX_CONVERSION_INPUT_LENGTH`(=4) error predicate, `raw_buf_index += convertedlen`, `input_buf_len += strlen(dst)` ⇒ `conv.converted.len()`. `dstlen = INPUT_BUF_SIZE - len + 1` preserved. |
| 7 | `CopyConversionError` (532) | `CopyConversionError` 320 | MATCH | Asserts; non-transcoding ⇒ `report_invalid_encoding`; transcoding ⇒ re-run with noError=false (`conversion_error_raise`); final `elog(ERROR,"encoding conversion failed without error")` ⇒ `errmsg_internal`. Always raises. |
| 8 | `CopyLoadRawBuf` (589) | `CopyLoadRawBuf` 349 | MATCH | Alias asserts; copy-down `memmove`⇒`copy_within`; `raw_buf_len -= raw_buf_index; raw_buf_index=0`; alias input adjust; `CopyGetData(.., 1, RAW_BUF_SIZE-len)`; NUL pad; `bytes_processed += inbytes`; progress update; `inbytes==0 ⇒ raw_reached_eof`. Scratch-buffer marshalling for the seam read is byte-identical. |
| 9 | `CopyLoadInputBuf` (649) | `CopyLoadInputBuf` 393 | MATCH | `nbytes=INPUT_BUF_BYTES`; alias raw_buf_index sync; `for(;;)` loop: `CopyConvertBuf`, return-when-`>nbytes`, `input_reached_error⇒CopyConversionError`, `input_reached_eof⇒break`, else `CopyLoadRawBuf`. |
| 10 | `CopyReadBinaryData` (700) | `CopyReadBinaryData` 430 | MATCH | Fast path `RAW_BUF_BYTES>=nbytes` memcpy; else do/while loop with `Min(nbytes-copied, RAW_BUF_BYTES)`, EOF break. `while(copied<nbytes)` ⇒ post-loop `if copied>=nbytes break`. |
| 11 | `NextCopyFromRawFields` (746) | `NextCopyFromRawFields` 476 | MATCH | Delegates to internal with `opts.csv_mode`. |
| 12 | `NextCopyFromRawFieldsInternal` (770) | 483 | MATCH | Header-line (`cur_lineno==0 && header_line` ⇒ `!= COPY_HEADER_FALSE`), `COPY_HEADER_MATCH` field-count + per-column `namestrcmp` checks, null-field-name error, `cur_lineno++`, `CopyReadLine`, EOF-at-start-of-line ⇒ None, attribute parse. All error messages/SQLSTATEs match. |
| 13 | `NextCopyFrom` (870) | `NextCopyFrom` 575 | MATCH | `MemSet` values/nulls/defaults ⇒ init `AttrValue{null,true}` + clear defaults; `CopyFromOneRow` dispatch ⇒ `copy_from_one_row`; defaults loop `values[defmap[i]] = ExecEvalExpr(defexprs[defmap[i]])`. |
| 14 | `CopyFromTextOneRow` (915) | 626 | MATCH | Delegates `is_csv=false`. |
| 15 | `CopyFromCSVOneRow` (923) | 635 | MATCH | Delegates `is_csv=true`. |
| 16 | `CopyFromTextLikeOneRow` (936) | 645 | MATCH | Overflow check (`attr_count>0 && fldct>attr_count`), per-attr loop, `convert_select_flags` skip, CSV FORCE_NOT_NULL/FORCE_NULL handling, `cur_attname`/`cur_attval`, defaults vs `InputFunctionCallSafe`, ON_ERROR soft-error path (`num_errors++`, verbose NOTICE via `notice_skipping_row` + `CopyLimitPrintoutLength`, `relname_only` toggle, early `return true`). |
| 17 | `CopyFromBinaryOneRow` (1085) | 767 | MATCH | `cur_lineno++`, `CopyGetInt16` EOF, `fld_count==-1` EOF-marker + trailing-data check, `fld_count != attr_count` error, per-attr `CopyReadBinaryAttribute`. |
| 18 | `CopyReadLine` (1157) | `CopyReadLine` 825 | MATCH | `resetStringInfo`⇒clear, `line_buf_valid=false`, `CopyReadLineText`, FRONTEND drain loop on EOF + buffer resets, EOL trim per `eol_type` (NL/CR drop 1, CRNL drop 2) with asserts, `line_buf_valid=true`. |
| 19 | `CopyReadLineText` (1233) | `CopyReadLineText` 887 | MATCH | Full state machine: CSV quote/escape toggle (`quotec==escapec ⇒ escapec=0`), `\r` CSV lookahead refill, embedded-EOL `cur_lineno++`, `\r` CRNL/CR/NL detection + carriage-return errors, `\n` detection + newline errors, `\\` (text only) `\.` end-of-copy-marker handling with CRNL extra-char step, alone-on-line checks, `REFILL_LINEBUF`. Macros `IF_NEED_REFILL_AND_NOT_EOF_CONTINUE`/`_EOF_BREAK`/`REFILL_LINEBUF` reproduced as inline helpers with call-site continue/break; all extralen=0 (matches every C call site). `input_byte` honors the `input_is_raw` aliasing. |
| 20 | `GetDecimalFromHex` (1535) | `GetDecimalFromHex` 1191 | MATCH | digit ⇒ `hex-'0'`; else `tolower(hex)-'a'+10`. |
| 21 | `CopyReadAttributesText` (1563) | 1204 | MATCH | Zero-column special case, `attribute_buf` reserve (idiomatic range model replaces force-large + pointer stability), field scan with `\\` de-escape (`\0-7` octal up to 3, `\x` hex up to 2, `\b\f\n\r\t\v`, default literal), `saw_non_ascii`⇒`pg_verifymbstr`, NULL-marker / DEFAULT-marker (`list_nth_int`, defexpr-or-error), trailing `\0` push **after** marker checks (C order), final pop of last `\0`. `0o377`/`0xff` masks correct. |
| 22 | `CopyReadAttributesCSV` (1817) | 1374 | MATCH | Zero-column case; not-quote/in-quote nested loops with `goto endfield`⇒labeled break; escape-peek (`nextc==escapec||quotec`), unterminated-quote error, trailing `\0` push **before** marker check (C `endfield` order), `!saw_quote` gate on NULL marker, DEFAULT marker. The kept-for-fidelity dead `end_ptr=cur_ptr` in the in-quote loop is annotated. |
| 23 | `CopyReadBinaryAttribute` (2012) | 1563 | MATCH | `CopyGetInt32` EOF error, `fld_size==-1` ⇒ NULL via `ReceiveFunctionCall(NULL)`, `fld_size<0` invalid-field-size error, load `fld_size` bytes (EOF error), `ReceiveFunctionCall(buf)`, `cursor != len` ⇒ `INVALID_BINARY_REPRESENTATION` error. Cursor tracked via `attribute_cursor`. |
| M1 | `IF_NEED_REFILL_AND_NOT_EOF_CONTINUE` (97) | `need_refill_and_not_eof` 1122 | MATCH | Test arm `ptr+extralen >= len && !hit_eof`; continue/undo-fetch at call sites. |
| M2 | `IF_NEED_REFILL_AND_EOF_BREAK` (109) | `need_refill_and_eof` 1137 | MATCH | Test arm `ptr+extralen >= len && hit_eof`; extralen always 0 ⇒ no partial-char consume, matching call sites. |
| M3 | `REFILL_LINEBUF` (126) | `refill_linebuf` 1106 | MATCH | Append `input_buf[index..ptr]` to `line_buf`, advance `input_buf_index`; aliasing-aware. |
| C1 | `BinarySignature[11]` (139) | `BINARY_SIGNATURE` (types_copy) | MATCH | `"PGCOPY\n\377\r\n\0"` = `[P,G,C,O,P,Y,\n,0o377,\r,\n,\0]`. Verified byte-for-byte. |

## 2. Constants / enums spot-check (against headers)

- `RAW_BUF_SIZE = 65536`, `INPUT_BUF_SIZE = 65536` (copyfrom_internal.h)
- `MAX_CONVERSION_INPUT_LENGTH = 4` (mb/pg_wchar.h)
- `MAX_COPY_DATA_DISPLAY = 100` (copyfrom.c)
- `ISOCTAL`/`OCTVALUE`/`IS_HIGHBIT_SET(0x80)`
- Enums `CopyHeaderChoice` (FALSE=0,TRUE,MATCH), `CopyOnErrorChoice` (STOP=0,IGNORE),
  `CopyLogVerbosityChoice` (SILENT=-1,DEFAULT=0,VERBOSE), `CopySource`,
  `EolType` — discriminants match commands/copy.h + copyfrom_internal.h
- SQLSTATEs: all error paths use `ERRCODE_BAD_COPY_FILE_FORMAT` except the binary
  trailing-bytes case which uses `ERRCODE_INVALID_BINARY_REPRESENTATION` — matches C

## 3. Seam + wiring audit

**Owned seam crates: none.** Per step-3 ownership-by-C-source-coverage, a
`crates/X-seams` is owned only if `X` maps to a C file in this unit's
`c_sources`. The only `c_source` is `copyfromparse.c`; the seam crate
`backend-commands-copyfrom-seams` maps to `copyfrom.c` (a *different*,
not-yet-ported unit that owns the `CopyFromStateData` and installs these seams
from its `init_seams()`). The parser unit therefore owns no inward seam crate.

- `backend_commands_copyfromparse::init_seams()` is **empty** and is correct: the
  unit owns no seam declarations, so there is nothing to `set()`. It is wired
  into `seams-init::init_all()` (`crates/seams-init/src/lib.rs:23`), preserving
  the aggregator pattern. No `set()` calls exist outside an owner. No finding.
- Every outward seam call crosses to a genuinely unported subsystem:
  - data-source read legs (`copy_get_data_{file,frontend,callback}`) → libpq/file I/O,
  - encoding (`pg_encoding_verifymbstr`, `pg_encoding_max_length`,
    `pg_do_encoding_conversion_buf`, `report_invalid_encoding`, `pg_verifymbstr`,
    `conversion_error_raise`) → mb subsystem,
  - `pgstat_progress_update_bytes_processed` → pgstat,
  - list/tupdesc accessors (`list_length`, `list_nth_int`, `attnumlist_ints`,
    `relation_natts`, `attr_info`, `namestrcmp_attr`) → list/relcache,
  - fmgr value layer (`input_function_call_safe`, `receive_function_call`,
    `exec_eval_expr`, `in_function_slot`, `typioparam`, `defexpr`,
    `notice_skipping_row`) → fmgr/executor,
  - `receive_copy_begin` → libpq frontend.
  `GetDatabaseEncoding` is consumed from `backend-utils-mb-mbutils-seams`.
  Each declaration is a thin signature (marshal + one delegate); no branching,
  node construction, or parser computation lives in any seam declaration. No
  finding.
- The byte-exact codec — buffer state machine, line reader, text/CSV/binary
  tokenizers, binary readers — is implemented **in-crate** over owned `Vec<u8>`
  buffers; nothing the parser owns was pushed across a seam to "somewhere else".

## 3b. Design conformance

- **Inherited opacity (types.md 6–7):** the heterogeneous cross-subsystem objects
  (`attnumlist` List, `ExprContext`, per-column `FmgrInfo`/`ExprState`,
  `escontext`, source `FILE *`/callback/`fe_msgbuf`) are carried as opaque keyed
  token newtypes (`ListHandle`, `ExprContextHandle`, `FmgrInfoSlot`,
  `ExprStateHandle`, `EscontextHandle`, `CopyFileHandle`, `DataSourceCbHandle`,
  `StringInfoHandle`); NULL ⇒ `Option<Token>`. `rel` is the shared
  `types_rel::Relation` alias, not an invented stand-in. No invented opacity.
- **Mcx + PgResult on allocating/erroring seams:** every seam that can `palloc`
  or `ereport` returns `PgResult<T>`; the in-crate functions thread `PgResult`
  with `?`.
- **No shared statics / ambient globals:** all per-backend parse state lives in
  the caller-owned `CopyParseState`; no module statics, no registry side tables.
- No locks across `?`, no unledgered divergence markers.

## 4. Verdict

**PASS.** All 23 functions + 3 macros + signature constant are `MATCH` or
properly `SEAMED` (`ReceiveCopyBegin`, `CopyGetData` — both genuine libpq/I/O
subsystem delegations, not displaced parser logic). Zero seam findings, zero
design-conformance findings. 14 in-crate codec tests pass. Eligible to mark
`audited` in `CATALOG.tsv`.
