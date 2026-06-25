# pgrust → WASM feasibility map (single-user / PGlite-style)

Branch: `wasm-support` (do NOT push to origin/main).

---

# PASS 12 (2026-06-24): output-adapter fixes (Goal 1) + 16-file pass/fail sweep (Goal 2)

## Goal 1 — formatter/harness fixes (all native-tested, then 1 wasm reverify)
- **Trailing-blank preservation** (`psql_format::format_aligned`): psql's
  `print_aligned_text` does NOT blanket right-trim a data row. Replaced the bogus
  `trim_end_matches(' ')` with psql's exact `finalspaces` rule — every non-last
  column gets full padding + trailing margin; the LAST column gets no trailing
  alignment pad (left-aligned) and no trailing margin space; cell values
  (incl. `char(n)`/`bpchar` blank-pad spaces and NULL right-align padding) are
  written verbatim. Verified vs char.out (`  ` blank row), text.out (` doh!`),
  arrays.out (NULL int4 = `     `), aggregates.out (NULL text = ` `).
- **Error field order** (`format_error`): libpq emits the `LINE n:`/caret block
  BEFORE `DETAIL:`/`HINT:`. Reordered to ERROR → LINE/caret → DETAIL → HINT.
- **Datetime env** (`tools/wasm-harness/src/main.rs`): the harness now forces the
  GUCs pg_regress sets — `datestyle='Postgres, MDY'`,
  `timezone=America/Los_Angeles`, `intervalstyle=postgres_verbose`.
- **psql backslash commands** (`main_loop.rs` regress reader): `\getenv`/`\set`/
  `\pset` were fed to the SQL parser → bogus `syntax error at or near "\"`. Now
  intercepted (echoed, not parsed). `\pset null '(null)'` additionally sets a
  NULL display string threaded into `format_aligned` (was hard-coded "").
- **Trailing line-comment off the query string**: psql sends only the text up to
  the terminating `;`; a trailing `-- comment` is echoed but not part of the
  query, so the server `LINE n:`/caret echo must not show it. The reader now
  trims it before sending.

Result: **char, text, int4, boolean = 0 difflines, 0 traps** (all four baselines
byte-exact). `psql_format` has 15 byte-exact native unit tests.

## Goal 2 — 16-file pass/fail sweep through the wasm module (`--regress-output`)
Run = `test_setup.sql` + a unique boundary marker + `<file>.sql` in one
single-user session; the marker slices the file's own output; best-of-expected
diff. (`scratchpad/mrun.sh`.)

| file     | difflines | classification |
|----------|-----------|----------------|
| char     | 0         | PASS |
| text     | 0         | PASS |
| int4     | 0         | PASS (was 21 — the wasm i32 lcm bug is gone on this branch) |
| boolean  | 0         | PASS (\pset null fix) |
| varchar  | 0         | PASS |
| int2     | 0         | PASS |
| oid      | 0         | PASS |
| case     | 0         | PASS |
| float4   | 14        | adapter-format: missing NOTICE/DETAIL diagnostic lines (values correct) |
| name     | 31        | adapter: `DO $$ … $$;` dollar-quote not tracked by the statement splitter |
| comments | 33        | adapter: multi-line `/* … */` block comment not tracked by the splitter |
| numeric  | 66        | adapter: `COPY … FROM stdin;` inline data not consumed as COPY data |
| strings  | 111       | mixed: trailing-comment LINE (improved), missing CONTEXT line, 1 wasm VFS `could not stat` error |
| select   | 545       | env: onek/tenk1 empty — `COPY … FROM :'filename'` needs psql `\set` var substitution (not implemented) |
| arrays   | 615       | env+adapter: same `:'filename'` COPY-from-file + `DO $$` splitting |
| float8   | 667       | wasm-trap: `unknown import: env::lgamma` — libm `lgamma` not stubbed in the wasm env; session aborts at the gamma test |

**8 / 16 PASS at 0 difflines.** The non-zero files fall into four buckets, none a
port-content bug (the same code is 230/230 native):
1. **Statement-splitter gaps** (name, comments, arrays-`DO`): the regress reader
   doesn't track dollar-quoting or `/* */` block comments, so those constructs
   mis-split into per-line syntax errors. Localized fix in `buffer_ends_statement`.
2. **COPY/psql-var** (select, arrays, numeric): `COPY … FROM :'filename'` needs
   psql `\set`/`:'var'` substitution; `COPY … FROM stdin` needs inline-data mode.
   Both are harness/adapter features, not port bugs (onek/tenk1 are empty → all
   downstream rows diff).
3. **Diagnostic routing** (float4 NOTICE/DETAIL, strings CONTEXT): NOTICE/CONTEXT
   messages aren't routed to the regress output stream.
4. **wasm-env gaps** (float8 `lgamma` import, strings `could not stat`): missing
   libm host import / a VFS stat divergence — true wasm-environment work.

---

# PASS 11 (2026-06-24): int4-residual diagnosis + tablespace-stat errno (investigation)

Investigated the two remaining wasm blockers from PASS 10. Both are now precisely
localized (NOT what they first appeared to be); fixes scoped.

## Full test_setup progress (after the errno fix unblocked CREATE TABLESPACE)
With the errno fix, full `test_setup.sql` ran past CREATE TABLESPACE, the type/
function defs, and the `onek` COPY — then trapped at `VACUUM ANALYZE onek` on
ANOTHER bare `Instant::now()`: `count_nondeletable_pages` (vacuumlazy/truncate.rs)
timed its lock-check with `Instant::now()` (panics on wasm64). FIXED (host-clock
TimeInstant alias, ae14971e9) + swept/fixed `timeofday()` (current.rs). The common
`GetCurrentTimestamp`/`now()`/`clock_timestamp` were already host-clock (PASS 5);
remaining bare `*::now()` are background (checkpointer/pgarch) or tests.

## The int4 "lcm/arith" residual is NOT a wasm i32/u128 codegen bug
Earlier theory (u128 / overflowing_mul miscompiling on wasm64) is DISPROVEN by
direct on-wasm tests — every isolated op is correct on wasm:
- `int4 '5'`/`'0b101'`/`'0o17'`/`'0x1f'` → 5/5/15/31; `int4 '0b100101'` → 37
- `(-2147483648)::int4` → -2147483648
- `gcd(6,4)`→2, `gcd((-2147483648),0)` → **ERROR integer out of range (correct!)**
- `lcm(6,4)`→12, `lcm(330,462)`→2310, `lcm(-330,462)`→2310
All match the native server byte-for-byte. So the u128 parser, base prefixes,
INT_MIN literal, gcd/lcm overflow checks all work in isolation on wasm.
=> NARROWED to a **stateful wasm divergence in the gcd/lcm overflow cluster**.
With the FULL test_setup (errno fix), int4 is **433 lines byte-EXACT**, then the
21-line residual is entirely the `gcd()/lcm()` overflow tests (int4.sql ~150-172).
Reproduced in isolation (gcdtest.sql = int4.sql lines 149-172):
- The 7-row `gcd(a,b),…` VALUES table prints CORRECTLY on wasm (matches native).
- BUT the two STANDALONE `gcd((-2147483648),0)` / `gcd((-2147483648),(-2147483648))`
  that follow it produce **NO error** on wasm (native: both ERROR `integer out of
  range`), and the `lcm(a,b),…` VALUES table that follows WRONGLY errors (native:
  6-row table). The errors that DO appear are shifted onto later statements.
- Yet `gcd((-2147483648),0)` ERRORs correctly when run ALONE. So evaluating the
  preceding multi-row gcd VALUES table corrupts some process state that flips the
  subsequent overflow checks — a stateful wasm bug (suspect: a thread-local /
  static / memory-context side effect of the per-row function evaluation over the
  VALUES scan, mis-lowered on wasm). Deep; needs a focused debug (instrument the
  int4gcd_internal INT_MIN branch + the surrounding executor state on wasm).
This is the LAST int4 residual; everything else (echo, aligned tables, errors,
LINE/caret, the first 433 lines) is byte-exact.

## Tablespace trap = FIXED: current_errno() read std's inert errno on wasm
ROOT CAUSE (broader than tablespace): `current_errno()` and tblspc_fs
`errno_now()` read `std::io::Error::last_os_error()`, which on
wasm64-unknown-unknown is INERT (std's errno is never set → always 0). The
`wasm-libc-shim` file/stat/open shims set their OWN thread-local errno, so every
errno-branching path (tablespace `stat`==ENOENT→create, EMFILE retry loops,
ENOENT tolerance) silently saw 0 on wasm. FIX (0bf05e021): `current_errno()`
reads `wasm_libc_shim::errno()` under cfg(wasm); `errno_now()` delegates to it.
Broad leverage across all errno-classified file paths. (Original mis-diagnosis
below kept for the trail.)

### (superseded) earlier guess — host_stat errno-mapping (missing dir reported as errno 0)
`CREATE TABLESPACE` → `TablespaceCreateDbspace` `stat`s the (absent) tablespace
dir; it must get `ENOENT` to take the "create it" branch. On wasm it instead
errors `could not stat directory "...": operation successful (os error 0)` — the
stat seam saw `Failed(errno=0)` not `Failed(ENOENT)`, so it fell into the
hard-error `else`. The chain: harness `host_stat` returns `io_errno(&e)` =
`-raw_os_error()` for a missing path (should be `-ENOENT`), `host_ret` does
`set_errno(-r)`. The observed errno 0 means `errno_now()` read 0 at the seam —
either `raw_os_error()` came back 0/None for that path, or errno was cleared
between the failed `libc::stat` and the read. Fix scope: ensure the wasm
`stat`/`lstat` shim sets errno=ENOENT when `host_stat`<0 for a not-found path
(and the harness returns `-ENOENT` for `NotFound`), so the tablespace
create-dir branch fires. This unblocks the full `test_setup.sql` (tablespace +
COPY fixtures) → every file needing the full fixtures.

---

# PASS 10 (2026-06-24): psql-output adapter — int4 byte-diff 96 → 21 (format pipeline byte-clean)

The `--regress-output` psql `-X -a -q` emulation now drives real regression files
through the single-user wasm backend and byte-diffs against `expected/*.out`. Real
`int4.sql` went from 96 → 83 → 60 → 50 → 25 → **21 difflines**, and the remaining
21 are ALL one wasm-specific arithmetic bug (below) — the output FORMAT pipeline
(echo, aligned tables, errors) is byte-exact.

## The output adapter (committed, native-tested formatter + wasm integration)
- `backend-access-common-printtup::psql_format` — `format_aligned` (centered
  headers, `---+---` rule, right/left-aligned cells, NULL=empty, `(N row[s])`),
  `format_error` (`ERROR:`/`DETAIL:`/`HINT:`/`LINE n:`+caret), `echo_query`. 9
  byte-exact unit tests vs real expected fragments (iterated on native).
- `--regress-output` switch (single-user only): no `backend>` prompt/banner; the
  reader echoes input psql-style; the DestDebug receiver collects rows and emits
  `format_aligned`; errors route to `format_error`.

## psql `-X -a -q` behavior, derived EMPIRICALLY from the live server (decisive)
The blank-line + comment rules are subtle; settled by running real psql:
- **Source blank lines are NEVER echoed** (psql MainLoop skips empty lines before
  the `puts(line)` echo).
- **psql prints exactly ONE blank line after each query RESULT SET** (none after
  an error, none after a utility/DML statement). This is the sole source of the
  blank lines in `expected/*.out` — emitted by the regress DestReceiver after the
  aligned table.
- **Leading comment lines are echoed but NOT sent to the parser**, so the error
  position → `LINE n:` is relative to the statement alone (verified: `-- c` then
  `SELECT int4 'bad';` → `LINE 1`, not `LINE 3`).
- The test command is `psql -X -a -q -d DB < test.sql > out 2>&1`
  (`src/test/regress/pg_regress_main.c`).

## Remaining int4 gap = one WASM-SPECIFIC arithmetic bug (not format, not lcm logic)
The 21 residual difflines are entirely the cascade of ONE query:
`SELECT a,b,lcm(a,b),lcm(a,-b),lcm(b,a),lcm(-b,a) FROM (VALUES ...(-2147483648,0)...)`
returns `ERROR: integer out of range` on wasm but the correct 6-row table on
**native** (verified against the live native server — byte-matches expected).
So `int4lcm`/`int4gcd` logic is correct; the divergence is wasm-only integer
arithmetic in that path (suspect: `i32` negation/`%`/overflow-check semantics on
wasm for the INT_MIN / `-b` operands). A real wasm correctness bug to fix
separately; it does not implicate the output adapter.

---

# PASS 9 (2026-06-24): ERROR RECOVERY WORKS on wasm64 (no unwinding needed) ✅✅✅

PASS 8 proved wasm64 can't unwind (rustc/LLVM emit no wasm-EH). PASS 9 makes PG's
error recovery work ANYWAY, under `panic=abort`, by replacing the parser's
non-local panic escape with a flag-based termination on wasm — because the error
data is already recorded out-of-band.

## The fix (one file, cfg-gated, native unchanged)
`pgrust-gram-c2rust-fgram/src/support.rs`: the three error escapes
(`scanner_yyerror`, `base_yylex` lexer error, `errfinish`) all already record
message/SQLSTATE/cursor in the `CUR_*` thread-locals before escaping. Native
keeps `panic_any(ParserAbortSentinel)` + `catch_unwind`. On wasm,
`pgrust_gram_error_jump` instead sets an `ABORT_PENDING` thread-local and
returns; `base_yylex` feeds bison `YYEOF`(0) while the flag is set, so the parse
terminates via `base_yyparse`'s existing nonzero-return path; `raw_parser_bytes`
(wasm arm, no `catch_unwind`) sees `abort_pending()` and returns NIL; the caller
surfaces the recorded error as `Err`. No Result-threading through the c2rust LALR
machine was needed — the data channel already existed.

## Scope (measured)
The whole non-local escape funnels through ONE function with 3 call sites in ONE
file. Builtin/executor `ereport(ERROR)` already propagates as `PgResult::Err`
(value-based, no panic), so it needed NO change — it recovered the instant the
parser stopped aborting. Total conversion surface: ~5 cfg-gated edits, 1 file.

## Milestones — all VERIFIED at runtime on wasm64
- **(a) syntax error recovers:** `SELECT 1;` → `SELECT syntax error here;`
  (`ERROR: syntax error at or near "here"`) → `SELECT 2;` runs → exit 0.
- **(b) builtin elog(ERROR) recovers:** `SELECT int4 'abc';`
  (`ERROR: invalid input syntax for type integer: "abc"`) and `SELECT 1/0;`
  (`ERROR: division by zero`) both reported; following statements run. (Worked
  with the (a) change alone — these were already `PgResult::Err`.)
- **(c) real `boolean.sql` END-TO-END:** **549 statements processed, 232 ERRORs
  recovered, clean exit 0** — including all the intentional boolean error tests
  (`invalid input syntax for type boolean: "test"/"foo"/…`). Previously aborted
  at statement ~37. The file now runs to completion.

## Status
Single-user wasm64 now: boots → `SELECT 1` → DDL/DML/aggregates/arrays/text →
**recovers from BOTH parser and builtin errors and keeps the session alive** →
clean shutdown. This removes the dominant blocker (PASS 7/8) for running the
error-testing regression files. Native build stays clean (cfg-split verified).
Remaining for full regression: output is single-user (not psql) format so the
suite's byte-diff harness doesn't apply directly; and a few error sites in
grammar *actions* (the `errfinish` mid-action path) may still need the same
flag-guard if a regression file hits them — to be hardened as they surface.

---

# PASS 8 (2026-06-23): panic=unwind spike — DEFINITIVE VERDICT (codegen layer blocks it)

**Question:** can PG's `catch_unwind`-based error recovery work on wasm64 so a
statement that raises ERROR is reported and the session CONTINUES?

**Verdict: NO with the current toolchain — blocked at the rustc/LLVM codegen
layer.** A custom unwind target spec builds and *links*, but rustc still lowers
`__rust_start_panic` to `__rust_abort`; no wasm exception-handling is emitted, so
`catch_unwind` cannot catch a real panic. The build artifact is committed
(`tools/wasm-harness/wasm64-unwind.json`) so the experiment is reproducible.

## What was tried (each layer)
1. **Target spec (rustc):** `--print target-spec-json` → flip
   `"panic-strategy":"abort"` → `"unwind"` (`tools/wasm-harness/wasm64-unwind.json`).
   Build needs `-Zjson-target-spec`. ACCEPTED.
2. **build-std:** `-Zbuild-std=std,panic_unwind,panic_abort` +
   `RUSTFLAGS="-C link-arg=--allow-undefined -C target-feature=+exception-handling"`.
   The whole tree + the `postgres` bin **compile and link** (178 MB module at
   `target/wasm64-unwind/wasm-boot/postgres.wasm`). BUILDS.
3. **Codegen (THE BLOCKER):** the linked module contains **zero** wasm EH
   instructions — `wasm-tools print | grep -c '(tag |throw|try_table'` = 0 (same
   as the abort module). Disassembling `__rust_start_panic` shows
   `call __rust_abort; unreachable`. rustc only exposes `-Zemscripten-wasm-eh`,
   explicitly scoped to **`wasm32-unknown-emscripten`** — there is NO flag to
   enable wasm EH for `wasm*-unknown-unknown`. So `-Cpanic=unwind` +
   `+exception-handling` produce no unwinding; the panic runtime is still abort.
   (rustc 1.97.0-nightly 2026-04-26, LLVM 22.1.2.)
4. **Runtime (wasmtime):** wasmtime 27 / wasmparser 0.219 **do** support the
   EXCEPTIONS proposal (`EXCEPTIONS(1<<13)=true`, on by default) — so the runtime
   is NOT the blocker. It's moot because the module has no EH to run.

## Runtime tests (empirical, on the unwind module)
- `SELECT 1;` -> `SELECT bogus;` -> `SELECT 2;` -> **session CONTINUED**, exited 0.
  BUT this is NOT unwinding: `column "bogus" does not exist` is returned as a
  `Result::Err(PgError)` from parse-analyze (no panic), so the loop reports it
  and moves on — works under panic=abort already.
- Real `boolean.sql` (~15 error tests): **TRAPPED at the first error that uses
  the panic escape** — `bool 'test'` invalid input -> `scanner_yyerror` ->
  `pgrust_gram_error_jump` -> `panic_any(ParserAbortSentinel)` ->
  `__rust_start_panic` -> `__rust_abort` (the `catch_unwind` in `raw_parser_bytes`
  is bypassed). 36 statements ran (setup + early boolean), same as the abort
  build — **no improvement**, confirming unwinding is not active.

## The two error paths (why one "works" and one doesn't)
- **`Result::Err(PgError)`** — most analyze/plan/execute errors propagate as
  values. These ALREADY recover under panic=abort (no unwinding needed). This is
  why some errors look recovered.
- **panic (`panic_any(ParserAbortSentinel)` / the `ereport(ERROR)` longjmp
  model)** — used by the grammar/scanner and the c2rust `errfinish` emulation,
  recovered by `catch_unwind`. These REQUIRE real unwinding -> blocked on wasm.

## REALISTIC PATHS to wasm-passes-regression (assessment)
- **(a) Wait for toolchain.** Needs rustc wasm-EH codegen for
  `wasm*-unknown-unknown` (today only emscripten has it via
  `-Zemscripten-wasm-eh`). No bare-wasm nightly flag works TODAY.
- **(b) Make the error model not panic on wasm (thread Result through the panic
  paths).** Bounded subset = the parser error escape (`scanner_yyerror` /
  `pgrust_gram_error_jump`) + the `errfinish` longjmp emulation. The parser
  escape is the harder half: the panic is raised from inside the c2rust-generated
  `base_yyparse` (an LALR state machine); returning a `Result` instead needs an
  error channel threaded through ~every reduction — large/fragile. errfinish is
  more localized. **A partial (parser-only) version still leaves executor
  `ereport(ERROR)` (div-by-zero, constraint violations) aborting**, so a bounded
  subset is NOT enough to pass the error-testing regression files; BOTH halves
  must be converted. Estimate: substantial.
- **(c) setjmp/longjmp shim / asyncify / different runtime.** No clean win for
  bare wasm64 (no setjmp/longjmp without Emscripten; asyncify is for suspension,
  not exception recovery).

## RECOMMENDATION
**wasm-passes-regression is NOT achievable today on `wasm64-unknown-unknown`:**
rustc/LLVM emit no wasm exception-handling for the bare-wasm targets, and PG's
error model needs `catch_unwind` for `ereport(ERROR)`/parser aborts. Unblock via
one of: (1) rustc gaining wasm-EH codegen for `wasm*-unknown-unknown` (track
upstream; not in 1.97-nightly/LLVM 22); (2) retarget to
`wasm32-unknown-emscripten` (has `-Zemscripten-wasm-eh`, but reintroduces ILP32 +
the Emscripten runtime — a large retarget); or (3) convert BOTH pgrust
error-escape panics (parser + errfinish) to `Result`-threaded returns under
cfg(wasm) — substantial/invasive. Everything else for single-user wasm works
(boot, SELECT 1, DDL/DML/aggregates/arrays/text, clean shutdown, value-`Result`
errors); the unwind-recovery path is the one hard wall, and it's a
toolchain-maturity blocker, not a pgrust bug.

---

# PASS 7 (2026-06-23): iteration speedup + the error-recovery (panic) wall

## Iteration speedup (DONE) — `.cwasm` cache now survives across runs
The ~10-13 min cold cranelift compile of the 178 MB module was being paid on
*every* run because `WASMTIME_BACKTRACE_DETAILS=1` (needed for symbolized traps)
feeds into the compiled artifact's expected feature set, so toggling it
invalidated the mtime-keyed `.cwasm`. Fix: the harness now pins
`config.wasm_backtrace(true)` + `wasm_backtrace_details(Enable)` in `Config`
(not via the env var). Result: **one cold compile per wasm *rebuild*; every
rerun afterward is ~5 s** (cwasm deserialize) **with full symbolized traces.**
This is the single biggest workflow win.

## The error-recovery wall (panic=abort) — the dominant blocker for SQL coverage
pgrust models `ereport(ERROR)` and parser/syntax errors as a Rust panic
(`scanner_yyerror` → `pgrust_gram_error_jump` → `panic_any(ParserAbortSentinel)`;
`errfinish` similarly), which the tcop loop / `raw_parser_bytes` recover with
`catch_unwind`. **On `wasm64-unknown-unknown` this recovery cannot run:** the
target spec hard-codes `"panic-strategy":"abort"`, so a profile-level
`panic = "unwind"` is *silently overridden* — `catch_unwind` never fires and any
error aborts the whole process (verified: `SELECT 1` succeeds, then
`SELECT syntax error` → wasm `unreachable`).
- `-Zbuild-std=std,panic_unwind,panic_abort` *does* compile + link the full
  `postgres.wasm`, but the target override defeats it at runtime. Reverted the
  profile to `panic = "abort"`.
- **Consequence for coverage:** error-FREE statements run cleanly (SELECT, DDL,
  DML, aggregates, arrays, etc.); any statement that raises an error aborts. The
  upstream regress files deliberately include error cases (boolean ~15,
  create_table ~25 error-test statements), so they abort partway. `case`/`select`
  (≈0 error tests) run end-to-end.
- **The real fix (next, larger effort):** a CUSTOM target spec with
  `panic-strategy: unwind` + `-Ctarget-feature=+exception-handling` + wasmtime's
  exceptions proposal, so `catch_unwind` works and the existing C-faithful error
  model runs unchanged. Alternative (much larger) would be threading `Result`
  through the c2rust-generated `base_yyparse` + `errfinish` — not worth it vs the
  target-spec route.

## Real upstream regress files (harness can't take them verbatim)
`src/test/regress/sql/*.sql` use psql meta-commands (`\set`/`\getenv`/`\copy`),
`COPY FROM data/*.data`, and `regress.so` C functions. The single-user backend
(`postgres --single`) is not psql, so a faithful `setup_lite.sql` is derived from
the real `test_setup.sql`: psql `\`-lines dropped, `COPY ... FROM :'filename'`
rewritten to absolute host paths (the harness VFS `resolve()` host-path fallback
reaches them), and the one `regress.so` C function (`binary_coercible`) dropped.
Then the real test file is appended and fed on stdin.

---

# PASS 6 (2026-06-23): **`SELECT 1;` RETURNS A RESULT + CLEAN SHUTDOWN under wasmtime** ✅✅✅

The single-user backend now boots all the way through `InitPostgres` (control
file, WAL `StartupXLOG`, CLOG/multixact SLRU, relcache/catcache bootstrap from
`base/5/*` + `global/*`), reaches the `backend>` prompt, and **executes
`SELECT 1;` end-to-end**, printing the real `printtup` output:

```
backend>
	 1: ?column?	(typeid = 23, len = 4, typmod = -1, byval = t)
	----
	 1: ?column? = "1"	(typeid = 23, len = 4, typmod = -1, byval = t)
	----
backend>
```

i.e. parse → analyze → rewrite → plan → execute → DestRemote/printtup all run
correctly on wasm64. This is the headline single-user milestone.

## What landed this pass (all behind `cfg(target_family="wasm")`; native unchanged)
- **`set_max_safe_fds` fixed-path on wasm** — `count_usable_fds` `dup(2)`-probes
  the kernel fd table to size `max_safe_fds`. wasm64-unknown-unknown has no fd
  table (`dup` is an ENOSYS shim), so the probe reported 0 usable fds and the
  boot FATAL'd `insufficient file descriptors available to start server
  process`. Under cfg(wasm) skip the probe and treat `max_files_per_process`
  (default 1000) as fully usable — the host VFS hands out its own integer fds.
  (`backend-storage-file-fd/src/vfd_core.rs`)
- **`ValidateXLOGDirectoryStructure` routed through the VFS** — it used
  `std::path::Path::exists`/`is_dir` to verify `pg_wal`. `std::fs` is inert on
  wasm64 (does not reach the host VFS), so the check always failed →
  `FATAL: required WAL directory "pg_wal" does not exist` even though the datadir
  has `pg_wal`. `dir_exists`/`path_exists` now go through the fd VFS seams
  (`path_is_dir`/`file_exists`), which stat through the host on wasm and the real
  FS natively (matching the C `stat` test). **General lesson: any
  `std::fs`/`std::path::Path` existence/metadata check is a latent wasm bug —
  it must route through the fd/smgr VFS seam, not std.**
  (`backend-access-transam-xlog/src/startup.rs`)
- **Host-clock timing on two more `std::time` sites** — `do_checkpoint.rs`
  `wallclock_time()` (`SystemTime::now()`) and `backend-storage-sync`'s
  `instr_time_*` helpers (`Instant::now()`) were called unconditionally; both
  panic on wasm64 (unsupported time backend). Routed through
  `wasm_libc_shim::now_unix_nanos()` under cfg(wasm). These run during the
  checkpoint / `ProcessSyncRequests` fsync timing.

## Boot trace (boot7 → SELECT 1)
1. (carried from pass 5) module compiles/instantiates/boots ✅
2. **fd blocker:** `set_max_safe_fds` FATAL — FIXED (fixed-path above).
3. **pg_wal blocker:** `ValidateXLOGDirectoryStructure` FATAL — FIXED (VFS route).
4. boot reaches `backend>`, runs `SELECT 1;`, prints `?column? = "1"` ✅
5. **shutdown blocker — FIXED:** on stdin EOF the clean-exit path
   `proc_exit → shmem_exit → shutdown_xlog_cb → ShutdownXLOG` aborted because
   `shutdown_xlog_cb` called `backend_access_transam_xlog::ShutdownXLOG(code,arg)`
   — the deferred xlog-driver **panic stub** (`xlog_driver_deferred!`) — instead
   of the ported driver. (NB: this callback is registered only when
   `!IsUnderPostmaster`, so postmaster-managed backends in the native regress
   suite never hit it — the bug was latent to the standalone-exit path on every
   target, not wasm-specific.) Fixed by calling the real
   `do_checkpoint::ShutdownXLOG()` (installed elsewhere as the `shutdown_xlog`
   seam), which writes the shutdown checkpoint and flips the control file to
   `Shutdowned`.
6. **CLEAN SHUTDOWN ✅** — the boot now runs the *entire* single-user lifecycle:

```
backend> 1: ?column? = "1"  (typeid = 23, len = 4, typmod = -1, byval = t)
backend> LOG:  shutting down
LOG:  checkpoint starting: shutdown immediate
LOG:  checkpoint complete: shutdown immediate
[harness] guest exited cleanly with code 0.
```

   i.e. boot → `SELECT 1` (correct result) → `ShutdownXLOG` shutdown checkpoint
   (flushes pg_xact / pg_multixact / WAL seg / pg_control / pgstat through the
   host VFS) → `proc_exit(0)`. The wasm `instr_time` host-clock fix in
   `ProcessSyncRequests` held (the checkpoint's per-fsync timing did not trap).

## Path forward from here
- Multi-statement / DML / DDL exercise (so far only `SELECT 1`). Run a wider SQL
  script through the same harness and fix each wasm-specific surface as before
  (the recurring classes: inert `std::fs`/`std::path::Path` → route via the
  fd/smgr VFS seam; `SystemTime`/`Instant::now()` → `host_now_ns`; deferred
  panic-stub seams that only the standalone path reaches).
- A release/opt wasm profile to cut wasmtime's ~10-13 min cold compile.
- A browser OPFS/IndexedDB VFS behind the same `pgvfs` import shape.

## Harness note (iteration cost)
- `WASMTIME_BACKTRACE_DETAILS=1` changes the compiled artifact, so toggling it
  invalidates the mtime-keyed `.cwasm` and forces a full ~10-13 min cranelift
  recompile of the 178 MB wasm-boot module. Pick one mode and stick to it across
  a debugging session.

---

# PASS 5 (2026-06-23): **the module BOOTS — `postgres::main` runs under wasmtime** ✅

The VFS file seam is now real and the single-user binary boots under the
`tools/wasm-harness` wasmtime (memory64) host. Build/run recipe:

```
cargo +nightly build -Zbuild-std=std,panic_abort --bin postgres \
  --target wasm64-unknown-unknown
echo "SELECT 1;" | tools/wasm-harness/target/debug/pgrust-wasm-harness \
  target/wasm64-unknown-unknown/debug/postgres.wasm <datadir>
```

(NB: wasmtime's cold cranelift compile of the 278 MB *debug* module takes
~2-4 min per run — expected. A release/opt wasm build would be far faster +
smaller; not yet done.)

## What landed this pass (all behind `cfg(target_family="wasm")`)
- **Host-VFS file seam wired** — `wasm-libc-shim` routes the raw POSIX file
  syscalls (open/close/read/write/pread/pwrite/lseek/fsync/ftruncate/stat/lstat/
  fstat/unlink/mkdir/rmdir/rename/access/readlink + opendir/readdir/closedir) to
  `pgvfs` host imports the harness backs with real files under a preopened
  datadir. Real `WasmFile`/`WasmOpenOptions`/`WasmReadDir`/`WasmMetadata` carriers
  + an `fscompat` module stand in for `std::fs` (which is inert on wasm64).
  `backend-storage-file-fd` (+twophase/copydir/xlogrecovery) alias
  `OsFile -> std::fs::File` natively / `WasmFile` on wasm.
- **stdio/argv to the host** — std stdout/stderr/stdin are no-ops on wasm64;
  query results + LOG lines + the SQL input stream route through
  host_stdout/stderr/stdin. argv comes from `host_argc`/`host_argv` (no WASI
  argv). The `postgres` bin + tcop main_loop + printtup honor this.
- **`ChangeToDataDir` = no-op** — `std::env::set_current_dir` is unsupported on
  wasm64; the harness maps the datadir as its preopened root.
- **`proc_exit` host import** — `std::process::exit` traps (`unreachable`) on
  wasm64; routed to a host import (clean store shutdown). This also made the
  swallowed FATAL startup messages visible.
- **`geteuid/getuid` non-root** — `check_root` refused to boot because
  `geteuid()` was 0; now returns a fixed non-root `WASM_UID` (1000), with
  `WasmMetadata::uid()` matching so the `checkDataDir` ownership interlock passes.

## Boot trace (boot2 -> boot3)
1. module compiles + instantiates ✅
2. `__wasm_call_ctors` + `host_argc -> 11` (argv injected) ✅
3. **boot2 blocker:** `check_root` FATAL (`"root" execution not permitted`) —
   FIXED by the non-root uid.
4. **boot3 blocker:** `get_user_name_or_exit` (`getpwuid(geteuid()).pw_name`)
   FATAL `user does not exist` — FIXED: `getpwuid`/`getpwuid_r` synthesize a
   fixed `postgres` entry for `WASM_UID`.
5. **boot4 blocker:** `InitProcessGlobals → GetCurrentTimestamp →
   SystemTime::now()` PANIC (`std::sys::time::unsupported`). `Instant::now()` is
   equally unsupported. FIXED: a `host_now_ns` host import + `now_unix_nanos()`;
   `GetCurrentTimestamp`/`time`/`gettimeofday`/`instr-time`/`pg-strong-random`
   all read it.
6. **boot6 blocker:** `PostgresSingleUserMain` reached, then panicked *while
   emitting a log/error report*: `log_status_format → std::process::id()` PANIC
   (`std::sys::process::unsupported::getpid`). FIXED: log-prefix pid via the
   shim's `getpid`. (NB: a report was being emitted → some earlier ERROR is
   queued; the next boot will finally surface its text now logging doesn't panic.)
7. boot7: watching for the first real LOG/ERROR text + datadir I/O.

### Harness iteration note
The harness now caches the compiled module to `<wasm>.cwasm` (mtime-keyed) so
only the *first* boot after a wasm rebuild pays the ~3-13 min cranelift compile;
reuses are instant. The host imports (`pgvfs::*`, `host_proc_exit`,
`host_now_ns`) are resolved at instantiation, so a harness-only change (new
import) does NOT invalidate the .cwasm — but you must rebuild the harness binary.

## Path forward from here
- Walk the single-user startup (InitPostgres / lock-file / WAL-redo / relcache
  bootstrap) fixing each wasm-specific surface as it traps: the
  `--allow-undefined` TLS/XML/locale imports must never be *called* on this path
  (the harness traps them); the SysV-shm arena + no-op signals/sema already
  stand in for the postmaster IPC the single backend doesn't need.
- Eventually: a release/opt wasm profile (faster wasmtime compile, smaller
  module) and a browser OPFS/IndexedDB VFS behind the same `pgvfs` import shape.

---

# PASS 4 (2026-06-23): **the `postgres` binary LINKS for wasm64** ✅

**HEADLINE: `cargo +nightly build -Zbuild-std=std,panic_abort --bin postgres
--target wasm64-unknown-unknown` now produces a valid WebAssembly module —
`target/wasm64-unknown-unknown/debug/postgres.wasm` (278 MB, `\0asm` v1).**
Every one of the ~1140 crates compiles and the whole tree links. Native
`cargo build --bin postgres` stays clean (verified — Mach-O arm64 binary still
builds, 0 errors). All wasm code is behind `cfg(target_family = "wasm")` (build
scripts gate on `CARGO_CFG_TARGET_FAMILY == "wasm"`); the linker flag is scoped
to the wasm64 target only.

## How the last ~30-crate frontier + the link stage were cleared

### 1. One shared `wasm-libc-shim` crate (the keystone refactor)
Instead of copying a `libc_wasm` module into each of ~40 OS-coupling crates, a
single new crate **`crates/wasm-libc-shim`** (`lib name wasm_libc_shim`,
`#![cfg(target_family="wasm")]` internally → empty/inert natively) provides one
authoritative `libc` stand-in: every POSIX **const** (errno, `O_*`, `S_IF*`,
`SIG*`, `AF_*`, `LC_*`, `IPC_*`, …) with Linux/glibc values; every **type**
(`stat`, `iovec`, `sockaddr*`, `rlimit`, `tm`, `timeval`, `itimerval`, `passwd`,
`group`, `fd_set`, `FILE`, `locale_t`, …); and **functions** in two classes —
*real* (`malloc`/`calloc`/`realloc`/`free` over Rust's allocator; `memcpy`/
`memmove`/`memset`/`strlen`/`strcmp`/… mem+str builtins; the ASCII ctype/`*_l`
folds; `time`/`gettimeofday` via `SystemTime`; errno via a thread-local) and
*single-user no-op / ENOSYS-error* stubs (`fork`→`unimplemented!()`, the raw
file syscalls, sockets, signals, SysV shm). Each consumer brings it in per
module with `#[cfg(target_family="wasm")] use wasm_libc_shim as libc;`, which
shadows the extern-prelude `libc` only on wasm. The crate also exports
`osfd` (`RawFd`/`AsRawFd`/`FromRawFd`/`IntoRawFd`/`OsStrExt`/`ExitStatusExt`)
and `osfs` (`OpenOptionsExt`/`FileExt`/`MetadataExt`/`PermissionsExt`/
`OsStrBytesExt`) modules standing in for the absent `std::os::unix` /
`std::os::fd` on `wasm64-unknown-unknown`.

### 2. The file/VFS keystone (`backend-storage-file-fd`, `smgr-md`, …)
`fd.c` models open files as `std::fs::File` and converts to/from a raw fd via
`AsRawFd`/`FromRawFd`/`IntoRawFd` + `RawFd` + `OsStrExt` — none of which exist on
`wasm64-unknown-unknown`. Each import was cfg-split to `wasm_libc_shim::osfd`.
The raw-fd *conversions* are genuinely unsupportable on bare wasm64 (no fd backs
a `File`), so they `unimplemented!()` like `fork()` — single-user file I/O is
meant to route through a host VFS behind the smgr/fd **seams**, not these raw
libc entry points. The common open/read/write/stat/close path is left real-
signatured and ENOSYS-returning until the VFS is wired (next pass).

### 3. The link stage — native-lib `extern "C"` blocks
Once every crate compiled, `rust-lld -flavor wasm` ran and surfaced **162
undefined symbols** in 4 families: **OpenSSL** (`SSL_*`/`X509_*`/`BIO_*`/…),
**libxml2** (`xml*`), **locale-aware ctype** (`*_l`, `mbstowcs`, `nl_langinfo_l`,
`strcoll_l`), and a few libc misc (`fopen`/`lgamma`/`nanosleep`). Two fixes:
- **build-script guards** — `backend-utils-adt-{float-libm,xml-libxml}-ffi` and
  `backend-libpq-be-secure-openssl-ffi` `build.rs` now early-return on
  `CARGO_CFG_TARGET_FAMILY == "wasm"` so wasm-ld is no longer told to link the
  native `-lm`/`-lxml2`/`libcrypto.a` (the macOS `libcrypto.a` archive members
  aren't even wasm objects).
- **`--allow-undefined`** — scoped to `[target.wasm64-unknown-unknown]` in
  `.cargo/config.toml`, this turns the remaining unresolved C symbols
  (TLS/XML/locale-aware-ctype `extern "C"` decls) into wasm *imports*. Single-
  user `postgres --single` never reaches the TLS listener / XML / locale-aware
  paths, so they're never **called**; they only needed to resolve at link time.

## What this does NOT yet do (path to actually BOOTING `postgres --single`)
LINKING ≠ booting. To run the module in a wasm runtime:
1. **Wire a real VFS** behind the smgr/fd seams (the raw file syscalls and the
   `File`↔fd conversions are currently `unimplemented!()`/ENOSYS). On a hosted
   wasi runtime `std::fs` lowers to wasi; `wasm64-unknown-unknown` has no wasi,
   so the datadir I/O needs host-import functions (`open`/`read`/`write`/`stat`/
   `close`/`unlink`) wired into the file seams, or a target switch to a wasi64
   target if/when one ships.
2. **Gate the postmaster/aux/fork spawn** off the boot path and force the
   `postgres --single` entry (the `fork()`/`StartChildProcess` path must never
   execute — it's `unimplemented!()`).
3. **Force `io_method=sync`** (method_worker is unported) and confirm none of
   the `--allow-undefined` imports (TLS/XML) are hit on the single-user path.
4. **A host harness** (wasmtime/wasmer with memory64 enabled, or a browser
   memory64 build) providing the preopened datadir + stdin/stdout for the
   single-user query loop.

---

# PASS 3 (2026-06-23): the trivial-ports wave + waiteventset — build now reaches **1110 crates compiling**

Same target/recipe as Pass 2 (`cargo +nightly build -Zbuild-std=std,panic_abort
--bin postgres --target wasm64-unknown-unknown --keep-going`).

## What landed (all behind `cfg(target_family="wasm")`; native verified unchanged)

Pass-3 step-1 trivial pure-logic ports + the substantial step-3 waiteventset shim:

| Crate | wasm stub |
|---|---|
| `portability-instr-time` | `clock_gettime` → `std::time::Instant` monotonic anchor (+1 tick so the first reading is >0) |
| `port-pgstrcasecmp` | high-bit ctype fold (`islower`/`isupper`/`tolower`/`toupper`) → identity; the C/POSIX locale never folds a high-bit byte, and there is no locale on wasm |
| `port-noblock` | `fcntl(F_SETFL,O_NONBLOCK)` → successful no-op (single-user fds stay blocking) |
| `types-dfmgr` | `dev_t`/`ino_t` → local 64-bit aliases (`mod libc` shadow); the dynamic loader is inert single-user |
| `interfaces-libpq-legacy-pqsignal` | `sigaction` shim → no-op returning `SigDisposition::Default`; nothing in the backend calls this frozen client-ABI symbol |
| `port-pg-strong-random` | `/dev/urandom` read → SplitMix64 seeded from the clocks + buffer addr (NOT crypto-strong — bring-up stand-in; TODO route to a host `random_get` import). `clock_realtime_ns` → `SystemTime` |
| `common-wchar-fgram` | `libc::memchr`/`strnlen` → local C-ABI slice shims (identical signatures) |
| `backend-storage-ipc-waiteventset` | **the substantial one.** New third `imp` module (`cfg(wasm)`) alongside the kqueue (`not(linux), not(wasm)`) and epoll (`linux`) ones. No kqueue/epoll/signalfd, no `kill`/`SIGURG`: single-user has one backend thread, no listener socket, no postmaster. `wait_block` polls the registered `WL_LATCH_SET` latch directly (finite timeout → reports timeout if nothing ready; infinite wait → reports the latch ready-anyway rather than deadlocking the lone thread, since no external waker exists). `WL_SOCKET_*`/`WL_POSTMASTER_DEATH` never register single-user. `WakeupOtherProc`/`wakeup_my_proc`/`now_millis` cfg-split off `kill`/`clock_gettime`. |

## Build reach: ~33 crates (Pass 2) → **1110+ crates compiling** (Pass 3)

With the bin's `seams_init::init_all()` pulling in the whole tree, the
`--keep-going` build now **compiles 1110+ crates** before the residual errors,
and the binary does **not yet link**. Clearing the four cheap blockers this
lane (`backend-parser-small1`, `backend-utils-error-fgram`,
`backend-utils-misc-stack-depth`, `backend-postmaster-interrupt`) advanced the
frontier *further*: cargo halts a subtree at its first failure, so each fix
exposes the downstream crates that were previously never attempted (the failing
set shifted from the 27 below to ~30, now including the replication/walsender,
syslogger and `mmgr-fgram` crates that the earlier failures had masked). The
failures cluster into a small number of symbol families (counts = error
occurrences in the first full pass):

| Family | Top missing symbols | Failing crates | Plan |
|---|---|---|---|
| **File / VFS** (the keystone) | `open`/`close`/`stat`/`lstat`/`unlink`/`readlink`/`umask`/`O_*`/`S_IF*`/`mode_t`/`off_t` + errno (`ENOENT`/`EBADF`/`EIO`/`EMFILE`/`ENFILE`/`ENOSPC`…) | `backend-storage-file-fd` (fd.c), `backend-storage-sync`, `port-dynloader`, `backend-access-transam-timeline`/`xlogarchive`, `backend-backup-walsummary`, `backend-bootstrap-bootstrap` | **wasi preopened-dir VFS** behind the file seams; on `wasm64-unknown-unknown` (no wasi) use `std::fs` (which lowers to wasi when hosted) or a host-import VFS. This is now the single biggest remaining keystone. |
| **errno constants** | `ENOENT`/`EBADF`/`EIO`/`EEXIST`/`ENOSPC`/`EINTR`/`EAGAIN`/`EWOULDBLOCK`/… (incl. const match-patterns, E0531) | `backend-utils-error-fgram`, scattered | local `mod libc` errno consts with standard Linux numeric values (trivial; in flight this lane) |
| **ctype** | `tolower`/`isupper` | `backend-parser-small1` | identity fold (same as `port-pgstrcasecmp`; in flight) |
| **rlimit** | `getrlimit`/`RLIMIT_STACK`/`RLIM_INFINITY`/`rlim_t`/`rlimit` | `backend-utils-misc-stack-depth` | return `-1` ("rlimit unknown" → accept-anything; in flight) |
| **locale** | `locale_t`/`newlocale`/`freelocale`/`LC_CTYPE_MASK`/`wctype`… | `backend-utils-adt-pg-locale` | stub to the C/POSIX single locale (no `newlocale`); large but mechanical |
| **sockets** | `AF_UNIX`/`sockaddr`/`sockaddr_storage`/`socklen_t`/`IPPROTO_TCP`/`setsockopt` | `backend-libpq-pqcomm`, `backend-libpq-be-secure*` | cfg-out the listener/secure paths — single-user has no socket; query I/O is stdin/stdout |
| **signals** | `kill`/`sigprocmask`/`SIG_SETMASK`/`SIGTERM`/`SIGINT`/`SIGHUP`/`SIGQUIT`/`SIGUSR1/2`/`SIGCHLD`/`SIGPIPE`/`sigset_t` | `backend-postmaster-startup`/`interrupt`, `backend-storage-ipc-{pmsignal,procarray,sinval,shmem,dsm-core}`, `backend-storage-lmgr-proc`, `backend-access-nbtree-core` | no-op signal install/mask (mirror the landed `pqsignal`/`latch` stubs); `__errno_location` → `std::io::Error::last_os_error` |
| **fork / process** | `fork`/`getpid`/`_exit`/`exit`/`fflush` | `backend-postmaster-fork-process` | `fork` = `unimplemented!()` under cfg(wasm); the single-user path must never reach it (gate `StartChildProcess`/aux spawn off cfg(wasm) and force `postgres --single`) |
| **SysV shm residue** | `shmctl`/`shm_unlink` | `backend-storage-ipc-shmem`, `pg-locale` | extend the landed `sysv-shmem` heap-arena stub to cover these calls |
| **malloc family** | `malloc`/`calloc`/`realloc`/`free`/`time` | `backend-snowball-dict-snowball`, `backend-replication-logical-reorderbuffer` | route to Rust `alloc` (these are c2rust-literal allocations); `time` → `SystemTime` |

### Ranked next steps (pass 3 → linking single-user binary)
0. **DONE this lane:** errno consts (`backend-utils-error-fgram`, new
   `src/libc_wasm.rs`), ctype (`backend-parser-small1`), rlimit
   (`backend-utils-misc-stack-depth` → `-1`), and `backend-postmaster-interrupt`
   (`_exit`→`process::exit(2)`, local `SIGQUIT`). These exposed the
   newly-reachable downstream crates: `backend-postmaster-syslogger`,
   `backend-replication-{walsender,walreceiver,slot,logical-launcher,
   logical-slotsync,logical-reorderbuffer}`, `backend-utils-mmgr-fgram`.
1. **Continue the cheap families** on the newly-exposed crates (same errno /
   signal / file shims) — `mmgr-fgram` likely just needs the errno/malloc shim;
   the replication crates are low priority single-user (cfg-out or stub).
2. **Signals wave** — a shared no-op shim for `kill`/`sigprocmask`/`SIG*`/
   `sigset_t`/`__errno_location` across the ~8 storage/postmaster/nbtree crates.
   Mostly mechanical, mirrors the landed `backend-libpq-pqsignal`/`latch` stubs.
3. **Sockets cfg-out** — `pqcomm` listener + `be-secure*` (single-user has no
   listener; this is the Pass-1 "cfg out the socket crates" step, now reached).
4. **The file/VFS keystone** — `backend-storage-file-fd` (`fd.c`) + `sync` +
   the archive/timeline/walsummary/bootstrap consumers, behind the file seams.
   On a hosted wasi runtime `std::fs` Just Works; the browser needs an
   OPFS/IndexedDB VFS. **This is the largest remaining single item.**
5. **fork / postmaster gate** — `fork` → `unimplemented!()`; gate
   `StartChildProcess`/aux-process spawn/signal-install under cfg(wasm); force
   the single-user path (`postgres --single`, `io_method=sync`). Once the file
   + signal + socket families clear, this is what stands between here and a
   **linking** binary.
6. **locale** stub (`pg-locale`) — C/POSIX single locale, no `newlocale`.
7. **malloc-family** crates (`snowball`, `reorderbuffer`) → Rust `alloc`.

---

# PASS 2 (2026-06-23): wasm64 is the target — pointer-width wall is GONE

## The decisive experiment (DONE) — wasm64 compiles the pointer-width crates UNMODIFIED

Target now: **`wasm64-unknown-unknown`** (nightly + `-Zbuild-std`). Build recipe:

```
rustup toolchain install nightly && rustup component add rust-src --toolchain nightly
cargo +nightly build -Zbuild-std=std,panic_abort -p <crate> --target wasm64-unknown-unknown
```

(Use `-Zbuild-std=std,panic_abort` — pgrust pulls in `pgrust-trace` which uses
`std`; `core,alloc` alone fails on it. wasm64 has no prebuilt std, so build-std
is mandatory.)

**VERDICT: all three pass-1 pointer-width-blocked crates compile under wasm64
with ZERO source changes to their pointer-width logic:**

| Crate | wasm32 blocker | wasm64 result |
|---|---|---|
| `backend-access-common-heaptuple` | `usize::from_ne_bytes([…8…])` (8B array, 4B usize) | **compiles unmodified** — usize is 8B on wasm64 |
| `pgrust-pg-ffi-fgram` | 64-bit `repr(C)` layout `const_assert!`s | **layout asserts PASS** — only socket-stub fixes needed (below) |
| `types-dsa` | `1usize << 40` overflows 32-bit usize | **compiles unmodified** — takes the native width-40 path (wasm64 ≠ `target_arch="wasm32"`) |

**Consequence: the entire ILP32 Datum rework is AVOIDED.** pgrust's hard 8-byte
`Datum` / 64-bit layouts are correct as-is on wasm64. This is the big
simplification the pass-1 doc hoped for. wasm64/memory64 is the target; wasm32 is
abandoned for this port.

> cfg note: wasm64 is NOT `target_arch="wasm32"`. ALL wasm cfgs use
> **`target_family = "wasm"`** (matches both wasm32 and wasm64). The pass-1
> `pg_rusage` exemplar was migrated from `target_arch="wasm32"` →
> `target_family="wasm"`. The ONE genuinely-wasm32-specific cfg kept as
> `target_arch="wasm32"` is `types-dsa`'s width-28 path (only correct for 4-byte
> usize; wasm64 must stay 40).

## Stubs landed this pass (compile on wasm64 AND native unchanged)

Foundational OS-coupling crates, behind `cfg(target_family="wasm")`, single-process model:

| Crate | What the wasm stub does |
|---|---|
| `pgrust-pg-ffi-fgram` | `PGSQL_AF_INET/INET6` pinned to on-disk literals (2/3); `SockAddr` gets local `sockaddr_storage`/`socklen_t` (libc lacks them) |
| `backend-utils-misc-pg-rusage` | getrusage → zeros (now `target_family="wasm"`) |
| `port-pqsignal` | no-op `pqsignal_be` (records handler, no kernel install) |
| `backend-libpq-pqsignal` | `sigset_t = u64` bitmask; sig* ops bit-twiddle; SIG* = Linux numbers; sigprocmask = no-op |
| `backend-port-sysv-shmem` | "shared memory" = a leaked MAXALIGN'd heap region carrying a real `PGShmemHeader` (one address space); detach/in-use = no-op |
| `backend-port-sysv-sema` | semaphores = no-ops (single process, no contention); TryLock always succeeds |
| `backend-storage-ipc-latch` | `kill(SIGUSR1)` → no-op (no other process); rest already portable (atomics/RwLock/seams) |
| `common-ip` | getaddrinfo/getnameinfo stub returns `EAI_FAIL`; `sockaddr_family`/`is_all_zeros` still work |
| `interfaces-libpq-fe` | client `open_socket` (Tcp/UnixStream) cfg'd out; returns unsupported-connection error (no outbound sockets single-user) |
| `types-storage` | `PGShmemHeader` `pid_t`/`dev_t`/`ino_t` → local wasm aliases (Linux widths) |
| `backend-utils-error` | syslog → stderr; PIPE_BUF/dup2 syslogger redirection cfg'd out (see fork) |

## NEXT WAVE (surfaced by `--bin postgres --target wasm64 --keep-going`)

The build now reaches **far past** the pass-1 13-crate wall — into the
postmaster/startup/replication layer. The new halting crates and their missing
libc families:

| Crate | Missing symbols | Class | Stub plan |
|---|---|---|---|
| `backend-storage-ipc-waiteventset` | `kqueue`/`kevent`/`EVFILT_*`/`EV_*`/`NOTE_EXIT`, `kill`, `SIGURG`, `close`, `fcntl` | event mux (kqueue/epoll) | wasi-poll or single-fd synchronous poll (no multiplexing single-user); this is the biggest one — latch.c sits on it |
| `portability-instr-time` | `clock_gettime`, `CLOCK_MONOTONIC`, `clockid_t`, `timespec` | clock | use `std::time::Instant`/`SystemTime` (works on wasi) or local timespec + a monotonic shim |
| `port-noblock` | `fcntl`, `F_GETFL/F_SETFL`, `O_NONBLOCK` | fd flags | no-op (single-user fds aren't set non-blocking) |
| `port-pg-strong-random` | `open`/`read`/`close`, `O_RDONLY` (reads /dev/urandom) | RNG source | wasi `getrandom`/`random_get`, or `getrandom` crate |
| `port-pgstrcasecmp` | `islower`/`toupper`/`isupper`/`tolower` (ctype) | ctype | Rust `u8`/`char` ASCII case fns (pure, trivial) |
| `port-pg-strong-random`/`types-dfmgr` | `dev_t`/`ino_t` | POSIX types | same local-alias trick as types-storage |
| `common-wchar-fgram` | `libc::memchr`, `libc::strnlen` | libc str | `core`/slice equivalents (`iter().position`, manual nul-scan) |
| `interfaces-libpq-legacy-pqsignal` | `sighandler_t`, `SIG_DFL`/`SIG_IGN`, `sigaction` | signals | mirror the `port-pqsignal` no-op stub |
| `backend-utils-error-fgram`, `common-wchar-fgram` | (downstream of above) | — | clear once deps fixed |
| `backend-postmaster-startup` / `backend-postmaster-interrupt` | signals (`SIG*`, `kill`, `SIG_IGN/DFL`), `sigaction` | postmaster/fork | the real keystone wave — gate spawn/fork under cfg(wasm), route through `postgres --single` |
| `backend-replication-logical-reorderbuffer` | (POSIX bits) | replication | low priority for single-user; cfg-out or stub |

### Ranked next steps (pass 3)
1. **`portability-instr-time`** (clock → `std::time`) and the trivial pure-logic
   ports (`port-pgstrcasecmp` ctype, `common-wchar-fgram` str, `port-noblock`
   no-op, `types-dfmgr` type-aliases, `interfaces-libpq-legacy-pqsignal`
   no-op). These are quick and unblock several subtrees.
2. **`port-pg-strong-random`** → wasi `random_get` (real entropy on wasi).
3. **`backend-storage-ipc-waiteventset`** — the substantial one (kqueue/epoll →
   single-fd poll / wasi-poll). latch.c's real waits route through it.
4. **The postmaster/startup wave** — gate fork/spawn/signal-install under
   cfg(wasm); force `postgres --single` (no postmaster, no listener,
   `io_method=sync`). This is the path to a LINKING single-user wasm binary.
5. Wire a wasi preopened-dir VFS for the data dir.

---

# PASS 1 (historical) — wasm32-wasip1 probe

Target probed: `wasm32-wasip1` (cleanest std/wasi target, fastest blast-radius map).
Method: `cargo build --bin postgres --target wasm32-wasip1 --keep-going`.

> SUPERSEDED by Pass 2: the pointer-width wall below is resolved by targeting
> wasm64. Kept for the OS-coupling symbol inventory, which still applies.

## TL;DR

- **Leaf + foundational crates compile cleanly for wasm32** — `types-core`,
  `types-error`, `mcx` (the memory-context allocator), `types-dsa` (after a
  1-line geometry cfg). The bulk of pure-logic crates (~1400) have no OS coupling
  and should Just Work once the ~13 foundational OS crates are stubbed.
- **The build currently stops at a first wave of exactly 13 crates** — all of
  them the lowest-level OS-coupling seams. Everything above them never got a
  chance to compile (cargo halts the dependency subtree).
- **Two root problems**, in order of severity:
  1. **`wasm32` = 32-bit pointers (THE hard blocker).** PostgreSQL's `Datum` is
     an 8-byte machine word and dozens of `repr(C)` structs are laid out for
     64-bit pointers. On wasm32 `usize`/pointers are 4 bytes, so `Datum`
     deform/form code (`usize::from_ne_bytes([…8…])`) and byte-exact layout
     asserts fail. This is **architectural, not a stub** — the faithful fix is
     to target **wasm64 / memory64** (64-bit pointers), not wasm32.
  2. **OS syscalls absent on wasi** — `fork`, SysV shm/sem, POSIX signals,
     `kqueue`/`epoll`, `UnixStream`. These ARE the seams the blueprint expects to
     stub for single-process mode; mechanical but voluminous (~2000 LOC across 4
     crates). The `libc` crate *links* on wasi but exposes none of these symbols.

## Per-crate failure map (the 13-crate first wave)

| Crate | Root cause | Class | Single-process fix |
|---|---|---|---|
| `backend-port-sysv-shmem` | `shmget/shmat/shmdt/shmctl`, `IPC_*` absent | SysV shm | cfg-stub: "shared memory" = plain `Box`/`Vec` in the single address space |
| `backend-port-sysv-sema` | `semget/semctl/semop`, `sembuf` absent | SysV sem | cfg-stub: no-op semaphores (single process, no contention) |
| `backend-libpq-pqsignal` | `sigset_t/sigprocmask/SIG*` absent | signals | cfg-stub: no-op signal mask/install |
| `port-pqsignal` | `sigaction/raise/SIG*/getpid` absent | signals | cfg-stub: no-op |
| `backend-storage-ipc-latch` | `kill/SIGUSR1/pid_t` absent | signals/IPC | cfg-stub: latch = in-process flag (no cross-proc wakeup) |
| `backend-storage-ipc-waiteventset` | `kqueue`/`epoll` (`EVFILT_*`,`EPOLL*`,`kevent`) absent | event mux | cfg-stub: single-fd synchronous poll (no multiplexing needed single-user) |
| `common-ip` | `AF_UNIX/AF_INET/sockaddr_*/getaddrinfo` absent | sockets | cfg out (single-user mode has no listener; query I/O is stdin/stdout) |
| `interfaces-libpq-fe` | `std::os::unix::net::UnixStream` not on wasi | sockets | cfg out (client lib, not needed in-process) |
| `backend-utils-error` (syslogger) | `openlog/syslog/dup2/PIPE_BUF` absent | logging | cfg-stub: write to stderr only |
| `backend-utils-misc-pg-rusage` | `getrusage/rusage/RUSAGE_SELF` absent | rusage | **DONE** — stubbed to zeros (cosmetic CPU strings only) |
| `backend-access-common-heaptuple` | `usize::from_ne_bytes([…8…])` — Datum is 8B, usize is 4B on wasm32 | **32-bit ptr** | needs wasm64 OR a 64-bit Datum type decoupled from usize |
| `pgrust-pg-ffi-fgram` | `const_assert!(size_of::<…ptr-bearing…>())` — 64-bit layout | **32-bit ptr** | needs wasm64; layout asserts are correct for 64-bit only |
| `types-dsa` | `1usize << 40` overflows 32-bit usize | **32-bit ptr** | **DONE** — width 40→28 on wasm32 (DSA unused single-process) |

### libc symbol families that are missing on wasm32-wasip1
- SysV IPC: `shmget shmat shmdt shmctl semget semctl semop` + `IPC_CREAT/EXCL/RMID/NOWAIT`, `key_t shmid_ds sembuf`
- Signals: `sigaction sigprocmask sigemptyset sigaddset raise kill` + `SIG{TERM,INT,QUIT,HUP,ALRM,CHLD,USR1,USR2,URG,WINCH}`, `SA_RESTART/NOCLDSTOP`, `sighandler_t sigset_t`
- Event mux: `kevent kqueue` + `EVFILT_{READ,WRITE,SIGNAL,PROC}`, `EV_{ADD,DELETE}`, `NOTE_EXIT`; `epoll*`/`EPOLL*` on the Linux path
- Sockets: `AF_UNIX AF_INET SOCK_STREAM sockaddr_un sockaddr_in addrinfo getaddrinfo EAI_*`
- Misc: `getrusage getpid dup2 openlog/syslog/closelog PIPE_BUF`

## What compiles today (verified)
`types-core`, `types-error`, `mcx`, `types-dsa` (post-fix),
`backend-utils-misc-pg-rusage` (post-fix) — all build for `wasm32-wasip1` AND
still build natively. Hundreds of intermediate seam crates (`*-seams`,
`types-startup`, `guc-tables`, `pg-prng`, …) were observed compiling in the same
pass before the first wave halted the subtree.

## The biggest hard parts (ranked)

1. **32-bit vs 64-bit pointers (wasm32 vs wasm64).** This is the real wall. A
   faithful PG port wants 8-byte `Datum` and 64-bit `repr(C)` layouts. Options:
   - **(preferred) target wasm64/memory64.** Rust support is tier-3/experimental
     and wasip1 is wasm32-only today; needs `-Zbuild-std` + nightly + the
     `memory64` proposal. This makes the layout/Datum problems vanish.
     PGlite ships **wasm32** because C Postgres there is compiled `ILP32`-clean
     (PG actually supports 32-bit builds: `SIZEOF_VOID_P==4`, `Datum` is then a
     32-bit `uintptr_t`, and `int8`/Datum-pass-by-ref differs). pgrust currently
     hard-assumes 64-bit Datum, so matching PGlite means **either** making Datum
     width target-dependent (large, invasive) **or** going wasm64.
   - **(invasive) make `Datum`/layouts 32-bit-clean** like a real PG ILP32 build.
     Touches ~22 SIZEOF_* sites, ~19 `usize::from_ne_bytes` sites, 6 layout
     asserts, and the by-ref/by-val pass convention. Large but mirrors a path
     C Postgres already supports.

2. **ASYNCIFY / blocking I/O vs a JS event loop.** wasi (Preview 1) gives
   synchronous blocking I/O, which suits single-user mode and `postgres --single`
   nicely (no event loop fighting). But the *browser* deliverable (emscripten +
   ASYNCIFY, as PGlite uses) needs every blocking read in the query I/O loop to
   be suspendable. wasip1 sidesteps this for a CLI/wasmtime host; the browser
   port is a separate, harder phase.

3. **The VFS.** The data dir + WAL need a virtual filesystem. wasip1 gives a real
   preopened-dir FS via wasmtime (works out of the box for a CLI host). The
   browser needs an IndexedDB/OPFS-backed FS (emscripten FS or a custom VFS
   behind `backend-storage-file`/`smgr` seams). pgrust's seam layering is the
   right insertion point.

4. **The single-user query loop.** `postgres --single` reads SQL from stdin and
   writes results to stdout — no postmaster, no fork, no listener. Gating out the
   postmaster/parallel-worker/aux-process spawn under cfg(wasm) and forcing
   `io_method=sync` is mechanical and matches the blueprint.

## Ranked next steps (to get a single-user wasm build to LINK)

1. **Decide wasm32 vs wasm64.** This gates everything. Recommendation: prototype
   **wasm64-unknown-unknown** (nightly + `-Zbuild-std`) to confirm the 32-bit
   crates (`heaptuple`, `pgrust-pg-ffi-fgram`) compile unmodified — if so, the
   feasibility story is dramatically simpler and the layout/Datum work disappears.
2. **Stub the 4 signal crates** (`port-pqsignal`, `backend-libpq-pqsignal`,
   `backend-storage-ipc-latch`, partly `waiteventset`) as no-ops under cfg(wasm).
   Follow the `pg_rusage` exemplar pattern (this commit).
3. **Stub SysV shm/sem** (`backend-port-sysv-shmem` → process-memory arena;
   `backend-port-sysv-sema` → no-op). This is the blueprint's `ShmemInitStruct`
   → `Box`/`Vec` step.
4. **cfg-out the socket crates** (`common-ip`, `interfaces-libpq-fe`) — single-
   user mode has no listener.
5. **Stub syslogger** (`backend-utils-error`) to stderr.
6. Re-run the full build; the next wave will be the postmaster/fork crates
   (`fork` ~3 files) and `WaitEventSet`/latch consumers — gate the spawn paths
   under cfg(wasm) and route the query loop through `postgres --single`.
7. Wire a wasi preopened-dir VFS for the data dir; force `io_method=sync`.

## Notes
- `nix` crate: **0 crates depend on it** (the blueprint's worry about `nix` w/o
  wasi support is moot here — pgrust uses `libc` directly, 97 crates).
- `fork`: lives in the postmaster/aux-process crates above the first wave; not
  yet reached by the build.
- All wasm changes so far are behind `cfg(target_arch="wasm32")`; native
  `cargo build --bin postgres` is unaffected.
