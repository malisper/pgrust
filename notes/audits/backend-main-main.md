# Audit: backend-main-main

- Unit: `backend-main-main`
- C source: `src/backend/main/main.c` (single TU)
- Branch: `port/backend-main-main`
- Verdict: **PASS** (after one fix round)

## Function inventory

Enumerated every function definition in `main.c` (cross-checked against
`../pgrust/c2rust-runs/backend-main-main/src/main.rs`, which keeps everything the
build kept).

| C function (main.c) | C loc | Port location | Verdict | Notes |
|---|---|---|---|---|
| `main(argc, argv)` | 70 | `lib.rs::pg_main` | MATCH | Modeled as `pg_main(mcx, argv) -> PgResult<MainOutcome>`. Startup sequence and dispatch switch reproduced step-for-step. See detail below. |
| `parse_dispatch_option(name)` | 239 | `lib.rs::parse_dispatch_option` | MATCH | Scans `DISPATCH_OPTION_NAMES`, skips `DISPATCH_FORKCHILD` (non-EXEC_BACKEND), exact `strcmp` -> `==`, default `DISPATCH_POSTMASTER`. This is the unit's one inward seam, installed by `init_seams`. |
| `startup_hacks(progname)` | 279 | `lib.rs::startup_hacks` | MATCH | Entire body is `#ifdef WIN32`; non-Windows = empty body. Named no-op kept as the call-site hook (matches c2rust empty body). |
| `init_locale(name, cat, locale)` | 364 | `locale.rs::init_locale` | MATCH | `pg_perm_setlocale(cat, locale).is_none() && ... "C".is_none()` -> `elog(FATAL)`. FATAL severity + identical message string. Two-call short-circuit preserved. |
| `help(progname)` | 383 | `help.rs::help` | MATCH | Every `printf(_())` line reproduced verbatim, in order, including the `USE_SSL`-gated `-l` line (SSL built here), `PACKAGE_BUGREPORT`/`PACKAGE_NAME`/`PACKAGE_URL`. Render-and-return (C printf to stdout) matches `GucInfoMain` shape. |
| `check_root(progname)` | 442 | `lib.rs::check_root` | MATCH | `geteuid()==0` -> FATAL root message; `getuid()!=geteuid()` -> FATAL uid-mismatch message. C `exit(1)` modeled as `Err(FATAL)` for caller to print+exit. Non-WIN32 branch. Exact message strings. |
| `__ubsan_default_options()` | 508 | `lib.rs::ubsan_default_options` | MATCH | `!reached_main -> ""` else `getenv("UBSAN_OPTIONS")`. `reached_main` modeled as thread-local `REACHED_MAIN` (per-process flag, not a shared static). |

### `main` / `pg_main` step-by-step

- `reached_main = true` -> `REACHED_MAIN.set(true)` MATCH
- WIN32 crashdump handler: WIN32-only, absent here MATCH
- `progname = get_progname(argv[0])` -> `get_progname::call` (seam) MATCH
- `startup_hacks(progname)` MATCH
- `argv = save_ps_display_args(argc, argv)` -> `ps_status::save_ps_display_args` (direct, ported) MATCH
- `MyProcPid = getpid()` — DROPPED. No `MyProcPid` global exists in the repo (only docstring mentions); per-backend globals are not modeled as shared statics (architecture rule). Nothing to assign / no owner to delegate to; behavior-preserving. Documented in code.
- `MemoryContextInit()` -> `memory_context_init::call()?` (seam) MATCH
- `set_stack_base()` -> `set_stack_base::call()` (seam) MATCH
- `set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("postgres"))` -> `set_pglocale_pgservice::call(argv0, "postgres-18")` MATCH **(fixed; see findings)**
- six `init_locale` calls (COLLATE/CTYPE/MESSAGES/MONETARY="C"/NUMERIC="C"/TIME="C") MATCH — LcCategory enum, locale strings, order identical
- `unsetenv("LC_ALL")` -> `libc::unsetenv(c"LC_ALL")` MATCH
- standard-option pre-scan (`argc>1`): `--help`/`-?` -> help+exit(0) = `PrintAndExit(help)`; `--version`/`-V` -> versionstr+exit(0) = `PrintAndExit(PG_BACKEND_VERSIONSTR)`; `--describe-config` and (`argc>2` && `-C`) -> `do_check_root=false` MATCH
- `if do_check_root check_root(progname)` MATCH
- dispatch parse: `argc>1 && argv[1][0]=='-' && argv[1][1]=='-'` -> `parse_dispatch_option(&argv[1][2..])`; byte-level check on `arg1.as_bytes()` with `len()>=2` guard (avoids OOB the C relies on NUL-termination for) MATCH
- switch: CHECK->`BootstrapModeMain(...,true)`, BOOT->`(...,false)` (direct, ported); FORKCHILD->`unreachable` (C `Assert(false)` non-EXEC_BACKEND); DESCRIBE_CONFIG->`GucInfoMain()` (direct) returns `PrintAndExit`; SINGLE->`PostgresSingleUserMain(argc,argv,strdup(get_user_name_or_exit(progname)))` -> seam + `get_user_name_or_exit::call`; POSTMASTER->`PostmasterMain` (seam) MATCH
- trailing `abort()` (unreachable) -> `Ok(MainOutcome::Dispatched)` (effectively unreachable; arms diverge) MATCH

## Constants verified against headers / c2rust (not memory)

- `DispatchOption` enum order: CHECK=0, BOOT, FORKCHILD, DESCRIBE_CONFIG, SINGLE, POSTMASTER — matches `types_startup::DispatchOption` and c2rust constants (DISPATCH_CHECK=0 .. DISPATCH_POSTMASTER=5).
- `PG_BACKEND_VERSIONSTR = "postgres (PostgreSQL) 18.3\n"` — confirmed by c2rust `[u8;28] = b"postgres (PostgreSQL) 18.3\n\0"`.
- `PG_TEXTDOMAIN("postgres") = "postgres-18"` — confirmed by c2rust `b"postgres-18\0"` (= `"postgres" "-" PG_MAJORVERSION`).
- help text / `PACKAGE_*` strings — matched line-for-line against c2rust string literals.
- LcCategory values (COLLATE=1..TIME=5, MESSAGES=6) per c2rust `LC_*` constants.

## Seam audit

Owned inward seam crate: `backend-main-main-seams` declares exactly one seam,
`parse_dispatch_option`. It is installed by `backend_main_main::init_seams()`
(`backend_main_main_seams::parse_dispatch_option::set(parse_dispatch_option)`),
and `seams-init::init_all()` calls `backend_main_main::init_seams()`
(crates/seams-init/src/lib.rs:108). `init_seams` contains only the one `set()`.

Outward seam calls — each into a genuinely-unported owner (real dep cycle / not
yet ported), thin marshal+delegate, no logic in the seam path:
- `get_progname` (common-path-seams), `set_pglocale_pgservice` (common-exec-seams),
  `pg_perm_setlocale` (pg-locale-seams), `memory_context_init` (mmgr-mcxt-seams),
  `set_stack_base` + `postgres_single_user_main` (tcop-postgres-seams),
  `postmaster_main` (postmaster-postmaster-seams), `get_user_name_or_exit`
  (common-username-seams).

Direct calls into already-ported owners (no seam needed): `save_ps_display_args`
(backend-utils-misc-more), `GucInfoMain` (backend-utils-misc-help-config),
`BootstrapModeMain` (backend-bootstrap-bootstrap) — all verified present.

No own-logic stubs, no `todo!`/`unimplemented!`, no deferred-error escape. The
only `panic!` is `unreachable_dispatch` for the FORKCHILD arm, mirroring C's
`Assert(false)` under non-EXEC_BACKEND.

## Findings and fix round

1. **DIVERGES (fixed): wrong gettext text domain.** Port passed `"postgres"` to
   `set_pglocale_pgservice`, but `PG_TEXTDOMAIN("postgres")` expands to
   `"postgres-18"` (c2rust authoritative). Wrong domain breaks message
   localization lookups. Fixed in `lib.rs` to pass `"postgres-18"` with a comment.

After the fix, every function is MATCH (or seam-correct).

## Gates

- `cargo test -p backend-main-main`: 4 passed.
- `cargo test -p seams-init`: recurrence_guard `every_seam_installing_crate_is_wired_into_init_all` and `every_declared_seam_is_installed_by_its_owner` pass.
- `cargo check --workspace`: clean (only pre-existing unrelated warnings in backend-access-common-printtup).

Verdict: **PASS**.
