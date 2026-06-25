# Audit: backend-utils-error-elog

Unit: `backend-utils-error-elog` (`src/backend/utils/error/elog.c`, 3826 lines,
PostgreSQL 18.3).
Crates audited: `crates/backend-utils-error`, plus the new seam/type crates the
port introduced: `crates/backend-libpq-pqcomm-seams`,
`crates/backend-postmaster-syslogger-seams`, `crates/backend-storage-ipc-seams`,
`crates/backend-utils-error-small-seams`, `crates/types-dest`.
Cross-checked against `../pgrust/c2rust-runs/backend-utils-error-elog/src/elog.rs`.
Auditor: independent re-derivation from the C sources and headers (`elog.h`,
`postgres_ext.h`, `port.h`, `tcop/dest.h`, `postmaster/syslogger.h`,
`sys/syslimits.h`).

## Sanctioned design divergences audited against (crate docs, `lib.rs`)

The port replaces sigsetjmp/`PG_exception_stack` with `PgResult` propagation:
at ERROR, `errfinish` pops the frame and returns `Err(PgError)`; the catching
recovery block emits via `emit_error_report_for` and calls `FlushErrorState`.
Consequences verified to be confined to the documented set: the
no-handler→FATAL promotion in `errstart`, the `PG_exception_stack == NULL`
gate on errfinish's FATAL `whereToSendOutput` reset, `pg_re_throw`'s
no-outer-handler FATAL path, and the Interrupt/QueryCancel holdoff resets
(owned by the unported interrupt machinery). Other adaptations: GUC/process
globals owned in `config.rs` with setters (guc→error is acyclic); NLS disabled
(the non-`ENABLE_NLS` build); variadic messages preformatted by callers with
only `%m` expanded against the saved errno; timestamps in GMT (boot-default
`log_timezone`); win32-only code not ported; PANIC = `std::process::abort()`;
session/process context behind the `BackendLogContext` provider whose defaults
mirror C boot state; memory-context plumbing (palloc/pfree/assoc_context /
`MemoryContextSwitchTo` / `ErrorContext` reset) subsumed by ownership.

One additional adaptation, audited and accepted: the port's `ThrowErrorData`
doubles as the `ereport` seam / `ErrorBuilder::finish` channel, so it copies
`message_id`, `context_domain`, `saved_errno`, `hide_statement`, and
`hide_context` *when present* (the C ThrowErrorData assumes "message_id is not
available" and never copies these). For every C-parity input (fields absent)
the behavior is bit-identical; when fields are present they came from this
crate's own builder, where C would have set them on the frame directly via
errmsg()/errhidestmt()/etc. The copy is therefore the ereport path, not a
behavior change.

## Function inventory (every definition in elog.c)

| # | C function (elog.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `is_log_level_output` (:202) | `policy.rs::is_log_level_output` | MATCH | Branch-for-branch: LOG/LOG_SERVER_ONLY vs `log_min_level == LOG \|\| <= ERROR`; WARNING_CLIENT_ONLY never logged; `log_min_level == LOG` → `elevel >= FATAL`; else `elevel >= log_min_level`. Level constants verified against elog.h (DEBUG5=10 … PANIC=23). |
| 2 | `should_output_to_server` (:236) | `policy.rs::should_output_to_server` | MATCH | `is_log_level_output(elevel, log_min_messages)`; boot default WARNING in `config.rs`. |
| 3 | `should_output_to_client` (:245) | `policy.rs::should_output_to_client` | MATCH | `whereToSendOutput == DestRemote && elevel != LOG_SERVER_ONLY`; auth-in-progress → `>= ERROR`; else `>= client_min_messages \|\| == INFO`. `CommandDest` ordering verified against tcop/dest.h (DestNone=0 … DestExplainSerialize=12). |
| 4 | `message_level_is_interesting` (:273) | `policy.rs::message_level_is_interesting` | MATCH | `elevel >= ERROR \|\| server \|\| client`. |
| 5 | `in_error_recursion_trouble` (:294) | `stack.rs::in_error_recursion_trouble` | MATCH | `recursion_depth > 2` on the thread-local stack state. |
| 6 | `err_gettext` (:306) | — (identity) | MATCH | Non-`ENABLE_NLS` build: identity, confirmed in the c2rust rendering. Not reproduced (sanctioned). |
| 7 | `errstart_cold` (:327) | `stack.rs::errstart_cold` | MATCH | Pure `pg_attribute_cold` wrapper; delegates to `errstart`. |
| 8 | `errstart` (:343) | `stack.rs::errstart` | MATCH | Promotion order identical: CritSectionCount>0→PANIC; ERROR→FATAL on `ExitOnAnyError \|\| proc_exit_inprogress` (the `PG_exception_stack == NULL` reason is the sanctioned divergence); max over stacked frames. Output decisions then `elevel < ERROR && !server && !client → false`. recursion_depth++ then `>1 && elevel>=ERROR` branch (ErrorContext reset has no owned counterpart); `>2` → clear context stack + suppress statement (`debug_query_string = NULL` modeled as the suppression flag honored by `check_log_of_query`). Overflow at `frames.len() >= 5` → clear + ereport(PANIC "ERRORDATA_STACK_SIZE exceeded") + abort, recursion_depth left elevated as in C. Frame init: saved errno, domain default "postgres" (C stores `PG_TEXTDOMAIN("postgres")` = "postgres-18"; with NLS off the string is inert — never printed, never compared), default sqlerrcode by elevel (INTERNAL_ERROR / WARNING / SUCCESSFUL_COMPLETION, via `types_error::default_sqlstate_for_level`). |
| 9 | `errfinish` (:474) | `stack.rs::errfinish` | MATCH (sanctioned divergence at ERROR) | recursion_depth++/CHECK_STACK_DEPTH (`Err("errstart was not called")`); location stored (filename normalized); backtrace iff unset ∧ funcname ∧ matches_backtrace_functions; context callbacks innermost-first; ERROR → CritSectionCount=0, pop, `Err` (the documented PG_RE_THROW replacement; holdoff resets are the catcher's per the divergence note); below ERROR → emit (timestamp reset, emit_log_hook may only clear output_to_server, server log, frontend), pop; FATAL → DestRemote→DestNone reset (gate subsumed by divergence), flush, pgstat seam, `proc_exit(1)` seam; PANIC → flush + abort. `CHECK_FOR_INTERRUPTS()` at the end is owned by miscadmin/tcop (documented caller responsibility). |
| 10 | `errsave_start` (:630) | `stack.rs::errsave_start` | MATCH (fixed) | `None` context → `errstart(ERROR, domain)`; the C `!IsA(context, ErrorSaveContext)` arm is statically impossible with the typed `SoftErrorContext` parameter. error_occurred set; `!details_wanted → false`. Frame at LOG with sqlerrcode = INTERNAL_ERROR, errno saved, output flags false. Fix round 1: stack overflow now PANICs (`ERRORDATA_STACK_SIZE exceeded` + abort) exactly like `get_error_stack_entry`; the original port silently cleared the stack and continued. |
| 11 | `errsave_finish` (:682) | `stack.rs::errsave_finish` | MATCH | CHECK_STACK_DEPTH first; frame level >= ERROR → punt to `errfinish`; else recursion_depth++ around pop, location stored, level replaced with ERROR, delivered via `escontext.save()` (the palloc_object+memcpy). Backtrace/context callbacks deliberately skipped, as in C. |
| 12 | `get_error_stack_entry` (:752) | inlined into errstart / errsave_start / ReThrowError-equivalent / GetErrorContextStack | MATCH (inlined) | Overflow → clear + PANIC in both push sites that can be driven externally (errstart, errsave_start). errno saved at frame creation. Zero-init == `PgError::new` defaults. |
| 13 | `set_stack_entry_domain` (:779) | inlined (`errstart`/`errsave_start`) | MATCH | domain default "postgres", `context_domain = domain`. |
| 14 | `set_stack_entry_location` (:796) | `stack.rs::normalize_filename` + location assignment | MATCH | basename after last `/` then last `\\`, lineno, funcname. |
| 15 | `matches_backtrace_functions` (:826) | `config.rs::matches_backtrace_functions` / `BacktraceFunctionList::matches` | MATCH | NULL list → false; empty funcname → false; the `p += strlen(p)+1` walk reproduced: scan entries, **stop at the first empty entry** (so `",a"` hides `a`, matching the C `\0\0` semantics). Verified against the check-hook's output representation. |
| 16 | `errcode` (:854) | `stack.rs::errcode` | MATCH | CHECK_STACK_DEPTH; sets sqlstate. |
| 17 | `errcode_for_file_access` (:877) | `stack.rs::errcode_for_file_access` + `errno.rs::sqlstate_for_file_access` | MATCH | errno→SQLSTATE table re-derived: EPERM/EACCES/EROFS→INSUFFICIENT_PRIVILEGE; ENOENT→UNDEFINED_FILE; EEXIST→DUPLICATE_FILE; ENOTDIR/EISDIR/ENOTEMPTY→WRONG_OBJECT_TYPE; ENOSPC→DISK_FULL; ENOMEM→OUT_OF_MEMORY; ENFILE/EMFILE→INSUFFICIENT_RESOURCES; EIO→IO_ERROR; ENAMETOOLONG→FILE_NAME_TOO_LONG; default INTERNAL_ERROR. Uses the frame's saved errno. |
| 18 | `errcode_for_socket_access` (:954) | `stack.rs::errcode_for_socket_access` + `errno.rs::sqlstate_for_socket_access` | MATCH | `ALL_CONNECTION_FAILURE_ERRNOS` verified against port.h:122 (EPIPE, ECONNRESET, ECONNABORTED, EHOSTDOWN, EHOSTUNREACH, ENETDOWN, ENETRESET, ENETUNREACH, ETIMEDOUT) → CONNECTION_FAILURE; default INTERNAL_ERROR. |
| 19 | `errmsg` (:1071) | `stack.rs::errmsg` | MATCH | message_id = fmt; message = fmt with `%m` expanded against saved errno (the preformatted-string adaptation of EVALUATE_MESSAGE; errno restoration == expanding against `saved_errno`). |
| 20 | `errbacktrace` (:1093) | `stack.rs::errbacktrace` | MATCH | `set_backtrace(edata, 1)` on the current frame. |
| 21 | `set_backtrace` (:1117) | `report.rs::set_backtrace` | MATCH (platform backtrace adaptation) | `std::backtrace` replaces `backtrace_symbols`; captured → `"\n" + frames`, unsupported → the exact C fallback string `"backtrace generation is not supported by this installation"`. `num_skip` is advisory (the Rust capture renders its own frame list) — content of a backtrace is inherently platform/debug-info-dependent in C too. |
| 22 | `errmsg_internal` (:1158) | `stack.rs::errmsg_internal` | MATCH | message_id = fmt (C sets it); untranslated == translated with NLS off. |
| 23 | `errmsg_plural` (:1181) | `stack.rs::errmsg_plural` | MATCH (fixed) | Fix round 1: `message_id` is now always `fmt_singular` (C `edata->message_id = fmt_singular`); message = n==1 ? singular : plural (the non-NLS `dngettext` fallback), `%m` expanded. The original port recorded the picked form as message_id. |
| 24 | `errdetail` (:1204) | `stack.rs::errdetail` | MATCH | detail set, `%m` expanded; no message_id (C doesn't set one). |
| 25 | `errdetail_internal` (:1231) | `stack.rs::errdetail_internal` | MATCH | == errdetail with NLS off. |
| 26 | `errdetail_log` (:1252) | `stack.rs::errdetail_log` | MATCH | detail_log set. |
| 27 | `errdetail_log_plural` (:1273) | `stack.rs::errdetail_log_plural` | MATCH | n==1 pick; no message_id in C. |
| 28 | `errdetail_plural` (:1296) | `stack.rs::errdetail_plural` | MATCH | n==1 pick. |
| 29 | `errhint` (:1318) | `stack.rs::errhint` | MATCH | hint set. |
| 30 | `errhint_internal` (:1340) | `stack.rs::errhint_internal` | MATCH | == errhint with NLS off. |
| 31 | `errhint_plural` (:1361) | `stack.rs::errhint_plural` | MATCH | n==1 pick. |
| 32 | `errcontext_msg` (:1387) | `stack.rs::errcontext_msg` + `context_chain.rs::append_error_context` | MATCH | appendval=true semantics: existing context + `'\n'` + new line; earlier calls more closely nested. Uses `context_domain` only for translation, inert with NLS off. |
| 33 | `set_errcontext_domain` (:1413) | `stack.rs::set_errcontext_domain` | MATCH | default "postgres" (same inert-domain note as #8). |
| 34 | `errhidestmt` (:1433) | `stack.rs::errhidestmt` | MATCH | |
| 35 | `errhidecontext` (:1452) | `stack.rs::errhidecontext` | MATCH | |
| 36 | `errposition` (:1468) | `stack.rs::errposition` | MATCH | 0 stored as `None`; every reader compares `> 0` / `unwrap_or(0)`, so 0 and None coincide (negative values: C keeps them and all readers test `> 0`; port's `nonzero` keeps negatives as `Some`, same reader outcomes). |
| 37 | `internalerrposition` (:1484) | `stack.rs::internalerrposition` | MATCH | same convention. |
| 38 | `internalerrquery` (:1504) | `stack.rs::internalerrquery` | MATCH | `None` drops the entry; Some replaces (pfree+strdup == owned assign). |
| 39 | `err_generic_string` (:1534) | `stack.rs::err_generic_string` + `types_error::PgError::set_error_field` | MATCH | The five PG_DIAG fields dispatch (codes verified: 's','t','c','d','n' per postgres_ext.h); unknown field → `Err` "unsupported ErrorData field id: %d" with default INTERNAL_ERROR sqlstate == `elog(ERROR, ...)`. |
| 40 | `set_errdata_field` (:1570) | subsumed (field assignment) | MATCH | `Assert(*ptr == NULL)` is debug-only; owned assignment is the strdup. |
| 41 | `geterrcode` (:1583) | `stack.rs::geterrcode` | MATCH | CHECK_STACK_DEPTH → `Err`. |
| 42 | `geterrposition` (:1600) | `stack.rs::geterrposition` | MATCH | `unwrap_or(0)`. |
| 43 | `getinternalerrposition` (:1617) | `stack.rs::getinternalerrposition` | MATCH | |
| 44 | `pre_format_elog_string` (:1650) | `report.rs::pre_format_elog_string` | MATCH | saves errnumber + domain. |
| 45 | `format_elog_string` (:1659) | `report.rs::format_elog_string` | MATCH | dummy frame == direct `%m` expansion against the saved errnumber; domain only fed translation (NLS off). |
| 46 | `EmitErrorReport` (:1692) | `stack.rs::EmitErrorReport` / `emit_top_frame` / `emit_error_report_for` | MATCH | recursion_depth++ / CHECK_STACK_DEPTH; timestamp reset before any output; emit_log_hook gated on output_to_server and only allowed to clear it (enforced by `&PgError` + `&mut bool` signature); server log then frontend. `emit_error_report_for` is the recovery-block flavor for the popped `Err` value (PgResult divergence), recomputing output decisions exactly as `pg_re_throw`'s severity-change path does. |
| 47 | `CopyErrorData` (:1751) | `stack.rs::CopyErrorData` | MATCH | deep clone == struct memcpy + per-field pstrdup. |
| 48 | `FreeErrorData` (:1823) | `stack.rs::FreeErrorData` | MATCH | consume-and-drop. |
| 49 | `FreeErrorDataContents` (:1835) | — (subsumed by Drop) | MATCH (subsumed) | pure deallocation of owned strings. |
| 50 | `FlushErrorState` (:1872) | `stack.rs::FlushErrorState` | MATCH | stack cleared, recursion_depth = 0; ErrorContext reset has no owned counterpart. |
| 51 | `ThrowErrorData` (:1900) | `stack.rs::ThrowErrorData` | MATCH (documented ereport-channel adaptation) | errstart with edata's level/domain; `!errstart → Ok(())`; copy-if-present of sqlerrcode/message/detail/detail_log/hint/context/backtrace/schema..constraint/cursorpos/internalpos/internalquery onto the fresh frame; errfinish with edata's location. Extra fields copied when present (message_id, context_domain, saved_errno, hide flags) — see the adaptation note above the table; identical to C whenever those fields are absent, which is every C-parity call. |
| 52 | `ReThrowError` (:1959) | `stack.rs::ReThrowError` | MATCH (under PgResult divergence) | C pushes a copy and PG_RE_THROWs; with Err-propagation the value itself is returned as `Err(edata)`. `Assert(elevel == ERROR)` maps to a PANIC-level error on violation (assert mapping). |
| 53 | `pg_re_throw` (:2009) | `stack.rs::pg_re_throw` | MATCH (under PgResult divergence) | pops the in-flight frame and returns it as `Err`; the no-outer-handler FATAL promotion is part of the replaced sigsetjmp machinery (its output-decision recomputation survives in `emit_error_report_for`). Empty stack → PANIC-level "pg_re_throw tried to return" (the `ExceptionalCondition`). |
| 54 | `GetErrorContextStack` (:2064) | `stack.rs::GetErrorContextStack` | MATCH (under retired-`error_context_stack` divergence #10) | C control flow reproduced faithfully: `recursion_depth++`, `get_error_stack_entry()` (push scratch entry; overflow → clear + PANIC, matching errstart's inlined form), assoc_context = CurrentMemoryContext elided (no palloc arena in the owned model, same elision as errstart), callback walk over `error_context_stack`, then `errordata_stack_depth-- ; recursion_depth--` and `return edata->context`. recursion_depth is elevated around the (empty) walk so it feeds `in_error_recursion_trouble`. Under divergence #10 the `error_context_stack` callback chain is retired in favor of attach-on-propagation, so the walk fires no callbacks (exactly as errfinish's callback walk is elided) and the scratch entry's context stays empty → returns `None`. Re-audit 2026-06-13: prior audit listed this MATCH but the function body had been removed (the lone repo-wide DIVERGES the catalog flagged); the function is now present. Test `get_error_context_stack_walks_retired_chain` confirms `None` + balanced recursion_depth. |
| 55 | `DebugFileOpen` (:2116) | `report.rs::DebugFileOpen` | MATCH | empty OutputFileName → no-op; open(O_CREAT\|O_APPEND\|O_WRONLY, 0666) failure → FATAL with errcode_for_file_access + `could not open file "%s": %m`; isatty; freopen-stderr realized as dup2(fd,2) (same append semantics), failure → FATAL `could not reopen file "%s" as stderr: %m`; tty ∧ IsUnderPostmaster → dup2(fd,1), failure → FATAL `... as stdout: %m`. FATAL flows through ThrowErrorData → errfinish FATAL recovery. |
| 56 | `check_backtrace_functions` (:2172) | `config.rs::check_backtrace_functions` | MATCH | charset = [0-9_a-zA-Z, \n\t] (strspn set verified); invalid → `Err("Invalid character.")` (the GUC_check_errdetail); empty → `Ok(None)` (*extra = NULL); split on commas dropping space/\n/\t; empty entries preserved so the matcher reproduces the `\0\0` early-termination exactly (",a" hides a; trailing comma yields a terminating empty entry). guc_malloc-failure path has no owned counterpart. |
| 57 | `assign_backtrace_functions` (:2233) | `config.rs::assign_backtrace_functions` | MATCH | stores the processed list. |
| 58 | `check_log_destination` (:2242) | `config.rs::check_log_destination` | MATCH | SplitIdentifierString semantics inlined (quoted `""`-escape verbatim, unquoted downcased+trimmed, trailing separator → error, empty list ok) and verified against varlena.c behavior; keywords stderr/csvlog/jsonlog/syslog (HAVE_SYSLOG set on this platform) case-insensitive; eventlog correctly rejected (win32-only); unknown → `Err("Unrecognized key word: \"%s\".")`; bit values verified against elog.h (1/8/16/2). |
| 59 | `assign_log_destination` (:2306) | `config.rs::assign_log_destination` | MATCH | |
| 60 | `assign_syslog_ident` (:2315) | `syslog.rs::assign_syslog_ident` | MATCH | changed-ident check; closelog + openlog_done=false; owned CString == strdup (kept alive while the connection is open). |
| 61 | `assign_syslog_facility` (:2347) | `syslog.rs::assign_syslog_facility` | MATCH | same no-thrash logic; boot facility LOG_LOCAL0. |
| 62 | `write_syslog` (:2372, HAVE_SYSLOG) | `syslog.rs::write_syslog` | MATCH (fixed) | openlog(ident or "postgres", LOG_PID\|LOG_NDELAY\|LOG_NOWAIT, facility); seq++; split iff `syslog_split_messages && (len > 900 \|\| has \n)`; chunk loop: leading-\n skip with nlpos recompute, buflen = min(line-to-\n or len, PG_SYSLOG_LIMIT=900), mbcliplen → UTF-8 boundary clip (sanctioned), `<= 0 → return`, word-boundary backtrack, `[%lu-%d] %s` / `[%d] %s`; short path `[%lu] %s` / `%s` (via the `syslog(level, "%s", msg)` safe form). Fix round 1: the word-boundary `isspace` now matches C-locale isspace (includes `\v`); the original port used `is_ascii_whitespace`, which excludes 0x0B. |
| 63 | `GetACPEncoding` (:2484, WIN32) | — | MATCH (excluded) | win32-only; not in the audited build (absent from c2rust output of this configuration). |
| 64 | `write_eventlog` (:2498, WIN32) | — | MATCH (excluded) | win32-only. |
| 65 | `write_console` (:2588) | `report.rs::write_console` | MATCH | non-win32 body is exactly `write(fileno(stderr), line, len)` with result ignored. |
| 66 | `get_formatted_log_time` (:2666) | `report.rs::get_formatted_log_time` | MATCH | cached; gettimeofday saved once per report; format re-derived: C writes `%Y-%m-%d %H:%M:%S     %Z` and pastes `.mmm` at offset 19 → `YYYY-MM-DD HH:MM:SS.mmm TZ`; port emits the same with the boot-default GMT zone (sanctioned). Civil-from-days conversion verified (Howard Hinnant algorithm). |
| 67 | `reset_formatted_start_time` (:2704) | `report.rs::reset_formatted_start_time` | MATCH | |
| 68 | `get_formatted_start_time` (:2716) | `report.rs::get_formatted_start_time` | MATCH | MyStartTime via provider; cached; second-resolution format. |
| 69 | `check_log_of_query` (:2740) | `report.rs::check_log_of_query` | MATCH | `is_log_level_output(elevel, log_min_error_statement)` ∧ !hide_stmt ∧ query string present (honoring the recursion-trouble suppression that models `debug_query_string = NULL`). |
| 70 | `get_backend_type_for_log` (:2763) | `report.rs::get_backend_type_for_log` | MATCH | provider supplies the postmaster/bgworker/`GetBackendTypeDesc` choice (those globals belong to miscinit/postmaster); no-provider default "not initialized" == `GetBackendTypeDesc(B_INVALID)` boot state. |
| 71 | `process_log_prefix_padding` (:2785) | `report.rs::process_log_prefix_padding` | MATCH | `-` then digits; NULL on `%-` at end or format ending in the number; padding sign applied. (Digit accumulation clamps instead of overflowing `int` — C overflow is UB; unreachable divergence.) |
| 72 | `log_line_prefix` (:2816) | `report.rs::log_line_prefix` | MATCH | `log_status_format(buf, Log_line_prefix, edata)` with the None-until-GUC boot value. |
| 73 | `log_status_format` (:2825) | `report.rs::log_status_format` | MATCH | pid-change reset of line counter + formatted start time; line counter incremented before the NULL-format check, as in C; `%%`; `*p > '9'` padding pre-check; every escape re-derived: a/u/d (port-gated, "[unknown]" for empty), b, c (`%llx.%x`), p, P (leader only for active parallel workers; spaces otherwise — including the padding==0 no-op), l, m (forced timestamp reset), t, n (`secs.millis` from saved timeval), s, i (ps_display, port-gated), L ("[none]" without port; provider owns the getnameinfo caching), r (host(port) padding-combined exactly), h, q (return when no port), v (`%d/%u` when procNumber valid), x, e (unpack_sql_state), Q (query id as signed 64-bit, matching `PRId64`), default ignore; `%*s` byte-width space padding reproduced by `append_padded` (never truncates). |
| 74 | `unpack_sql_state` (:3210) | `report.rs::unpack_sql_state` + `types_error::unpack_sqlstate` | MATCH | five `PGUNSIXBIT` extractions (`(v >> 6k & 0x3F) + '0'`, verified against elog.h:68). |
| 75 | `send_message_to_server_log` (:3230) | `report.rs::send_message_to_server_log` | MATCH | prefix + `SEVERITY:  `; VERBOSE → sqlstate; message / "missing error text" with tab-indent; `at character` from cursorpos else internalpos (`> 0` tests); DEFAULT-verbosity block: detail_log else detail, hint, QUERY (internalquery), CONTEXT (!hide_ctx), VERBOSE → LOCATION (funcname+filename / filename-only forms), BACKTRACE; STATEMENT via check_log_of_query; syslog severity map verified (DEBUGs→LOG_DEBUG, LOG/LOG_SERVER_ONLY/INFO→LOG_INFO, NOTICE/WARNING/WARNING_CLIENT_ONLY→LOG_NOTICE, ERROR→LOG_WARNING, FATAL→LOG_ERR, PANIC/default→LOG_CRIT); eventlog branch win32-excluded; csvlog/jsonlog gated on `redirection_done \|\| B_LOGGER` with stderr fallback flag; stderr block `(dest & STDERR) \|\| whereToSendOutput == DestDebug \|\| fallback`, chunked iff `redirection_done && !B_LOGGER` else write_console; B_LOGGER → write_syslogger_file (seam). |
| 76 | `write_pipe_chunks` (:3470) | `report.rs::write_pipe_chunks` | MATCH (fixed) | **Fix round 1: was MISSING** — the original port replaced the body with a seam call to the unported syslogger unit; the function is defined in elog.c and its logic now lives in this crate. Header re-derived from syslogger.h and the c2rust constants: PIPE_HEADER_SIZE=9 (nuls[2] + u16 len + i32 pid + u8 flags), PIPE_CHUNK_SIZE = PIPE_BUF (512 macOS / 4096 Linux, both ≤ 64K), PIPE_MAX_PAYLOAD = CHUNK−9; flags PIPE_PROTO_DEST_STDERR/CSVLOG/JSONLOG = 0x10/0x20/0x40, IS_LAST = 0x01; full-payload chunks without IS_LAST, final chunk with it; one `write(2, …)` per chunk (atomicity), result ignored; pid from the context provider; `Assert(len > 0)` → debug_assert. |
| 77 | `err_sendstring` (:3521) | `report.rs::err_sendstring` | MATCH | NUL-terminated field append; the recursion-trouble ASCII path and pq_sendstring coincide for owned UTF-8 (no conversion subsystem in the path), as documented. |
| 78 | `send_message_to_frontend` (:3533) | `report.rs::send_message_to_frontend` | MATCH | protocol test `(FrontendProtocol >> 16) >= 3 \|\| == 0` (PG_PROTOCOL_MAJOR verified via c2rust); 'N' below ERROR else 'E' (PqMsg_NoticeResponse/ErrorResponse byte values); field order identical: S, V, C, M (always; "missing error text"), D, H, W, s, t, c, d, n, P (cursorpos>0), p (internalpos>0), q, F, L (lineno>0), R, terminator; detail_log intentionally omitted; v2 path `SEVERITY:  message\n` + trailing NUL (`buf.len + 1`); pq_flush at the end. pq_putmessage/pq_putmessage_v2/pq_flush are SEAMED (pqcomm.c owners). |
| 79 | `error_severity` (:3711) | `report.rs::error_severity` | MATCH | full level→string map incl. WARNING_CLIENT_ONLY→"WARNING", LOG_SERVER_ONLY→"LOG", default "???". |
| 80 | `append_with_tabs` (:3763) | `report.rs::append_with_tabs` | MATCH | tab after every `\n`. |
| 81 | `write_stderr` (:3782) | `report.rs::write_stderr` | MATCH | preformatted adaptation of the va_list surface; delegates to vwrite_stderr. |
| 82 | `vwrite_stderr` (:3797) | `report.rs::vwrite_stderr` | MATCH | non-win32 body: fprintf(stderr) + fflush(stderr) == locked write_all + flush. |

File-scope state audited alongside: `errordata[5]`/`errordata_stack_depth`/`recursion_depth`
(thread-local `StackState`, ERRORDATA_STACK_SIZE = 5), `saved_timeval`/`formatted_*`
/`log_line_number`/`log_my_pid`/`save_format_*` (`LogState` mutex),
`openlog_done`/`syslog_ident`/`syslog_facility`/`seq` (`SyslogState`),
`backtrace_function_list`, the GUC globals (`Log_error_verbosity = PGERROR_DEFAULT(1)`,
`Log_destination = LOG_DESTINATION_STDERR(1)`, `Log_line_prefix = NULL`,
`syslog_sequence_numbers = syslog_split_messages = true`) — boot defaults verified
against the C initializers, owned in `config.rs` with setters for the future owners.
`error_context_stack` → `context_chain.rs` (innermost-first walk preserved; RAII pop
replaces manual unlink); `PG_exception_stack` intentionally absent (PgResult);
`emit_log_hook` → `sink.rs` slot with the clear-only contract enforced by type.

## Seam audit

| Seam crate | Declarations | Owner (C) | Justification | Thin? | Installed by |
|---|---|---|---|---|---|
| `backend-utils-error-seams` | `ereport(PgError) -> PgResult<()>` | this unit | inward seam: lower-layer crates report errors without a dependency edge | yes — direct delegate to `stack::ThrowErrorData` | `backend_utils_error::init_seams()` (sole `set()`, called from `seams-init::init_all()`); verified the only `set()` for it in the tree |
| `backend-libpq-pqcomm-seams` | `pq_putmessage`, `pq_putmessage_v2`, `pq_flush` | pqcomm.c (unported) | real cycle: pqcomm.c itself ereports | call sites are marshal-only (`&[u8]` body + msgtype byte) | owner when it lands (panics loudly until then; unreachable under boot defaults `whereToSendOutput = DestNone`) |
| `backend-postmaster-syslogger-seams` | `write_syslogger_file` | syslogger.c (unported) | real cycle: syslogger.c ereports / uses elog facilities | single delegate call | owner when it lands; unreachable unless this process is the syslogger. (`write_pipe_chunks` was removed from this crate in fix round 1 — it belongs to elog.c and is now implemented in `backend-utils-error`.) |
| `backend-storage-ipc-seams` | `proc_exit(i32) -> !` | ipc.c (unported) | real cycle: ipc.c ereports | single delegate on the FATAL path | owner when it lands |
| `backend-utils-error-small-seams` | `write_csvlog`, `write_jsonlog` | csvlog.c / jsonlog.c (separate catalog unit `backend-utils-error-small`) | real cycle: csvlog/jsonlog call back into elog helpers | single delegate each, gated exactly as the C call sites | owner when it lands; unreachable under boot `log_destination = stderr` |
| `backend-utils-activity-pgstat-seams` | `pgstat_set_session_end_cause_fatal` (pre-existing crate) | pgstat (unported) | real cycle | the `DISCONNECT_NORMAL → DISCONNECT_FATAL` conditional is the owner's (it guards the owner's global) | owner when it lands |

No seam path contains branching, construction, or computation; no `set()` calls
outside owners; `seams-init::init_all()` invokes `backend_utils_error::init_seams()`.
`types-dest` (`CommandDest`) verified value-for-value against tcop/dest.h;
`types-error` additions (PG_DIAG bytes, PGERROR_*, LOG_DESTINATION_*) verified
against postgres_ext.h / elog.h.

## Findings and fixes (round 1)

1. **`write_pipe_chunks` MISSING** — body had been replaced by a seam call to
   the unported syslogger unit although the function is defined in elog.c.
   Fixed: full chunk-protocol implementation in `report.rs` (constants verified
   against syslogger.h and the c2rust rendering); seam declaration removed.
2. **`errsave_start` stack overflow DIVERGES** — silently cleared the stack;
   C PANICs via `get_error_stack_entry`. Fixed: PANIC + abort, identical to
   the errstart path.
3. **`errmsg_plural` message_id DIVERGES** — recorded the picked form; C always
   records `fmt_singular`. Fixed in `stack.rs` and in `ErrorBuilder::errmsg_plural`;
   `ErrorBuilder::errmsg_internal` also now records the message id (C does).
4. **`write_syslog` isspace DIVERGES (edge)** — `is_ascii_whitespace` omits
   vertical tab (0x0B), which C-locale `isspace` includes; affected the
   word-boundary split. Fixed with a C-locale `c_isspace` helper.
5. **`GetErrorContextStack` recursion_depth DIVERGES (minor)** — did not elevate
   `recursion_depth` around the callbacks; C does (feeds
   `in_error_recursion_trouble`). Fixed.

Re-audit 2026-06-13 (catalog DIVERGES tail):
6. **`GetErrorContextStack` MISSING (the lone repo-wide DIVERGES)** — a prior
   restructure replaced the function body with a comment ("no counterpart under
   attach-on-propagation"), but the audit row still listed it MATCH; the
   function was therefore absent. Restored a faithful port: C control flow
   reproduced exactly (recursion_depth bracket, scratch `get_error_stack_entry`
   push with overflow→PANIC, callback walk, pop, `return edata->context`). The
   `error_context_stack` callback chain is retired under sanctioned divergence
   #10, so the walk fires nothing and the function returns `None` — identical
   in shape to how errfinish's callback walk is elided. Added test
   `get_error_context_stack_walks_retired_chain`. Re-exported from `lib.rs`.

All fixes re-audited from scratch against the C and the c2rust rendering;
workspace builds clean and all tests pass (28 crate tests + 5 types-error tests).

## Verdict

**PASS** (after fix round 1). Every function in the inventory is MATCH (or
MATCH-subsumed for pure allocation/lifetime machinery, or excluded win32 code
outside the audited build configuration), the seams are thin, justified, and
correctly wired, and all constants were verified against the headers.
