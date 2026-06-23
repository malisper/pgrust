//! Family F6 — command-line switch processing and the GUC check/assign hooks
//! of `tcop/postgres.c`.
//!
//! Reproduced here:
//!   * `process_postgres_switches` (postgres.c:3789)
//!   * `set_debug_options`         (postgres.c:3678)
//!   * `set_plan_disabling_options` (postgres.c:3707)
//!   * `get_stats_option_name`     (postgres.c:3749)
//!   * `forbidden_in_wal_sender`   (postgres.c:5032)
//!   * the GUC check/assign hooks (postgres.c:3543-3669):
//!     `check_client_connection_check_interval`, `check_stage_log_stats`,
//!     `check_log_stats`, `assign_transaction_timeout`,
//!     `check_restrict_nonsystem_relation_kind`,
//!     `assign_restrict_nonsystem_relation_kind`
//!   * `restrict_nonsystem_relation_kind` — the C int global written by the
//!     assign hook and read by the GUC-tables seam.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use mcx::MemoryContext;
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, FATAL};
use types_guc::guc::{GucContext, GucSource};
use types_startup::DispatchOption;

use guc_tables::vars;
use guc_tables::GucHookExtra;

use crate::globals;

// `PqMsg_FunctionCall` (`libpq/protocol.h`) — the fastpath function-call
// request message-type byte.
const PqMsg_FunctionCall: u8 = b'F';

// ---- restrict_nonsystem_relation_kind C int global + flags ----

/// `#define RESTRICT_RELKIND_VIEW 0x01` (tcopprot.h).
const RESTRICT_RELKIND_VIEW: i32 = 0x01;
/// `#define RESTRICT_RELKIND_FOREIGN_TABLE 0x02` (tcopprot.h).
const RESTRICT_RELKIND_FOREIGN_TABLE: i32 = 0x02;

thread_local! {
    /// `int restrict_nonsystem_relation_kind;` (postgres.c) — the parsed bitmask
    /// the `assign_restrict_nonsystem_relation_kind` hook stores. Read through
    /// the `backend-utils-misc-guc-tables` seam.
    static RESTRICT_NONSYSTEM_RELATION_KIND: Cell<i32> = const { Cell::new(0) };
}

/// Reader for the `restrict_nonsystem_relation_kind` int global, installed into
/// the `backend-utils-misc-guc-tables` seam (replaces the unit's boot-state
/// placeholder, which returned `0`).
pub fn restrict_nonsystem_relation_kind() -> i32 {
    RESTRICT_NONSYSTEM_RELATION_KIND.with(Cell::get)
}

fn set_restrict_nonsystem_relation_kind(value: i32) {
    RESTRICT_NONSYSTEM_RELATION_KIND.with(|c| c.set(value));
}

thread_local! {
    /// `char *restrict_nonsystem_relation_kind_string;` (postgres.c) — the GUC's
    /// own `conf->variable` (the raw comma-list text). The check/assign hooks
    /// parse it into the `restrict_nonsystem_relation_kind` bitmask above; this
    /// cell is the string storage the GUC engine reads/writes. boot_val `""`.
    static RESTRICT_NONSYSTEM_RELATION_KIND_STRING: core::cell::RefCell<Option<alloc::string::String>> =
        const { core::cell::RefCell::new(Some(alloc::string::String::new())) };
}

fn restrict_nonsystem_relation_kind_string() -> Option<alloc::string::String> {
    RESTRICT_NONSYSTEM_RELATION_KIND_STRING.with(|c| c.borrow().clone())
}

fn set_restrict_nonsystem_relation_kind_string(value: Option<alloc::string::String>) {
    RESTRICT_NONSYSTEM_RELATION_KIND_STRING.with(|c| *c.borrow_mut() = value);
}

// ---- set_debug_options / set_plan_disabling_options / get_stats_option_name ----

/// `SetConfigOption(name, value, context, source)`.
fn set_config_option(
    name: &str,
    value: &str,
    context: GucContext,
    source: GucSource,
) -> PgResult<()> {
    guc_seams::set_config_option::call(name, value, context, source)
}

/// `set_debug_options(int debug_flag, GucContext context, GucSource source)`
/// (postgres.c:3678) — apply "-d N" command line option.
pub fn set_debug_options(debug_flag: i32, context: GucContext, source: GucSource) -> PgResult<()> {
    if debug_flag > 0 {
        let debugstr = alloc::format!("debug{debug_flag}");
        set_config_option("log_min_messages", &debugstr, context, source)?;
    } else {
        set_config_option("log_min_messages", "notice", context, source)?;
    }

    if debug_flag >= 1 && context == GucContext::PGC_POSTMASTER {
        set_config_option("log_connections", "all", context, source)?;
        set_config_option("log_disconnections", "true", context, source)?;
    }
    if debug_flag >= 2 {
        set_config_option("log_statement", "all", context, source)?;
    }
    if debug_flag >= 3 {
        set_config_option("debug_print_parse", "true", context, source)?;
    }
    if debug_flag >= 4 {
        set_config_option("debug_print_plan", "true", context, source)?;
    }
    if debug_flag >= 5 {
        set_config_option("debug_print_rewritten", "true", context, source)?;
    }
    Ok(())
}

/// `set_plan_disabling_options(const char *arg, GucContext context, GucSource
/// source)` (postgres.c:3707) — the "-f X" plan-method-disable switch.
pub fn set_plan_disabling_options(
    arg: &str,
    context: GucContext,
    source: GucSource,
) -> PgResult<bool> {
    let tmp = match arg.as_bytes().first().copied() {
        Some(b's') => Some("enable_seqscan"),
        Some(b'i') => Some("enable_indexscan"),
        Some(b'o') => Some("enable_indexonlyscan"),
        Some(b'b') => Some("enable_bitmapscan"),
        Some(b't') => Some("enable_tidscan"),
        Some(b'n') => Some("enable_nestloop"),
        Some(b'm') => Some("enable_mergejoin"),
        Some(b'h') => Some("enable_hashjoin"),
        _ => None,
    };
    if let Some(tmp) = tmp {
        set_config_option(tmp, "false", context, source)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// `get_stats_option_name(const char *arg)` (postgres.c:3749) — map a "-t X"
/// statistics switch to its GUC name. In C this consults `optarg`; here `arg`
/// is `optarg` and we read its second byte the same way.
pub fn get_stats_option_name(arg: &str) -> Option<&'static str> {
    let b = arg.as_bytes();
    match b.first().copied() {
        Some(b'p') => {
            // optarg[1]: "parser" vs "planner"
            match b.get(1).copied() {
                Some(b'a') => Some("log_parser_stats"),
                Some(b'l') => Some("log_planner_stats"),
                _ => None,
            }
        }
        Some(b'e') => Some("log_executor_stats"),
        _ => None,
    }
}

// ---- forbidden_in_wal_sender ----

/// `forbidden_in_wal_sender(char firstchar)` (postgres.c:5032) — reject
/// fastpath / extended-query messages on a replication connection.
pub fn forbidden_in_wal_sender(firstchar: u8) -> PgResult<()> {
    if walsender_seams::am_walsender::call() {
        if firstchar == PqMsg_FunctionCall {
            return Err(PgError::error(
                "fastpath function calls not supported in a replication connection",
            )
            .with_sqlstate(types_error::ERRCODE_PROTOCOL_VIOLATION));
        } else {
            return Err(PgError::error(
                "extended query protocol not supported in a replication connection",
            )
            .with_sqlstate(types_error::ERRCODE_PROTOCOL_VIOLATION));
        }
    }
    Ok(())
}

// ---- process_postgres_switches ----

/// `process_postgres_switches(argc, argv, GucContext ctx, const char **dbname)`
/// (postgres.c:3789) — parse the backend command-line switches as GUC settings.
///
/// The seam contract (`backend-tcop-postgres-seams`) carries `argv: &[String]`
/// and `ctx`; the C `*dbname` out-parameter (only ever consumed by
/// `PostgresMain`, which is F0a) is returned here as the captured database name.
pub fn process_postgres_switches(argv: &[String], ctx: GucContext) -> PgResult<Option<String>> {
    let secure = ctx == GucContext::PGC_POSTMASTER;
    let mut errs = 0;
    let mut dbname: Option<String> = None;

    // The C function takes `char *argv[]`; build a working copy we may advance
    // past a leading "--single".
    let mut argv: Vec<String> = argv.to_vec();

    let gucsource = if secure {
        // switches came from command line.
        // Ignore the initial --single argument, if present.
        if argv.len() > 1 && argv[1] == "--single" {
            // argv++; argc-- (drop the element at index 1, keeping argv[0]).
            argv.remove(1);
        }
        GucSource::PGC_S_ARGV
    } else {
        // switches came from client.
        GucSource::PGC_S_CLIENT
    };

    // getopt(argc, argv, "B:bC:c:D:d:EeFf:h:ijk:lN:nOPp:r:S:sTt:v:W:-:")
    let mut opt = Getopt::new(&argv, "B:bC:c:D:d:EeFf:h:ijk:lN:nOPp:r:S:sTt:v:W:-:");

    while let Some(flag) = opt.next() {
        match flag {
            'B' => set_config_option("shared_buffers", &opt.optarg_str(), ctx, gucsource)?,
            'b' => {
                // Undocumented flag used for binary upgrades.
                if secure {
                    init_small::globals::SetIsBinaryUpgrade(true);
                }
            }
            'C' => { /* ignored for consistency with the postmaster */ }
            '-' | 'c' => {
                // Error if the user misplaced a special must-be-first option.
                if flag == '-'
                    && main_seams::parse_dispatch_option::call(&opt.optarg_str())
                        != DispatchOption::DISPATCH_POSTMASTER
                {
                    return Err(PgError::error(alloc::format!(
                        "--{} must be first argument",
                        opt.optarg_str()
                    ))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR));
                }

                // ParseLongOption(optarg, &name, &value)
                let (name, value) = parse_long_option(&opt.optarg_str())?;
                let value = match value {
                    Some(v) => v,
                    None => {
                        if flag == '-' {
                            return Err(PgError::error(alloc::format!(
                                "--{} requires a value",
                                opt.optarg_str()
                            ))
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
                        } else {
                            return Err(PgError::error(alloc::format!(
                                "-c {} requires a value",
                                opt.optarg_str()
                            ))
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
                        }
                    }
                };
                set_config_option(&name, &value, ctx, gucsource)?;
            }
            'D' => {
                if secure {
                    // userDoption = strdup(optarg)
                    globals::set_user_doption(Some(leak(opt.optarg_str())));
                }
            }
            'd' => {
                // set_debug_options(atoi(optarg), ctx, gucsource)
                set_debug_options(atoi(&opt.optarg_str()), ctx, gucsource)?;
            }
            'E' => {
                if secure {
                    globals::set_echo_query(true);
                }
            }
            'e' => set_config_option("datestyle", "euro", ctx, gucsource)?,
            'F' => set_config_option("fsync", "false", ctx, gucsource)?,
            'f' => {
                if !set_plan_disabling_options(&opt.optarg_str(), ctx, gucsource)? {
                    errs += 1;
                }
            }
            'h' => set_config_option("listen_addresses", &opt.optarg_str(), ctx, gucsource)?,
            'i' => set_config_option("listen_addresses", "*", ctx, gucsource)?,
            'j' => {
                if secure {
                    globals::set_use_semi_newline_newline(true);
                }
            }
            'k' => set_config_option(
                "unix_socket_directories",
                &opt.optarg_str(),
                ctx,
                gucsource,
            )?,
            'l' => set_config_option("ssl", "true", ctx, gucsource)?,
            'N' => set_config_option("max_connections", &opt.optarg_str(), ctx, gucsource)?,
            'n' => { /* ignored for consistency with postmaster */ }
            'O' => set_config_option("allow_system_table_mods", "true", ctx, gucsource)?,
            'P' => set_config_option("ignore_system_indexes", "true", ctx, gucsource)?,
            'p' => set_config_option("port", &opt.optarg_str(), ctx, gucsource)?,
            'r' => {
                // send output (stdout and stderr) to the given file
                if secure {
                    // strlcpy(OutputFileName, optarg, MAXPGPATH)
                    init_small::globals::SetOutputFileNameStr(&opt.optarg_str());
                }
            }
            'S' => set_config_option("work_mem", &opt.optarg_str(), ctx, gucsource)?,
            's' => set_config_option("log_statement_stats", "true", ctx, gucsource)?,
            'T' => { /* ignored for consistency with the postmaster */ }
            't' => {
                let tmp = get_stats_option_name(&opt.optarg_str());
                if let Some(tmp) = tmp {
                    set_config_option(tmp, "true", ctx, gucsource)?;
                } else {
                    errs += 1;
                }
            }
            'v' => {
                // -v: standalone FrontendProtocol override.
                if secure {
                    init_small::globals::SetFrontendProtocol(
                        atoi(&opt.optarg_str()) as types_core::ProtocolVersion,
                    );
                }
            }
            'W' => set_config_option("post_auth_delay", &opt.optarg_str(), ctx, gucsource)?,
            _ => {
                errs += 1;
            }
        }

        if errs != 0 {
            break;
        }
    }

    // Optional database name should be there only if *dbname is NULL.
    if errs == 0 && dbname.is_none() && opt.argc().saturating_sub(opt.optind) >= 1 {
        // *dbname = strdup(argv[optind++])
        dbname = Some(opt.argv_at(opt.optind).to_string());
        opt.optind += 1;
    }

    if errs != 0 || opt.argc() != opt.optind {
        if errs != 0 {
            opt.optind -= 1; // complain about the previous argument
        }
        let bad = opt.argv_at(opt.optind).to_string();
        // spell the error message a bit differently depending on context.
        if init_small::globals::IsUnderPostmaster() {
            return Err(PgError::new(FATAL, alloc::format!(
                "invalid command-line argument for server process: {bad}"
            ))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_hint(alloc::format!(
                "Try \"{}\" --help\" for more information.",
                progname()
            )));
        } else {
            return Err(PgError::new(FATAL, alloc::format!(
                "{}: invalid command-line argument: {bad}",
                progname()
            ))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_hint(alloc::format!(
                "Try \"{}\" --help\" for more information.",
                progname()
            )));
        }
    }

    Ok(dbname)
}

/// `progname` (main.c global), via the seam.
fn progname() -> alloc::string::String {
    postgres_seams::progname::call()
}

/// `ParseLongOption(string, &name, &value)` (guc.c).
fn parse_long_option(string: &str) -> PgResult<(alloc::string::String, Option<alloc::string::String>)> {
    // The guc.c owner allocates in `mcx`; we copy out to owned `String`s. Use a
    // short-lived context for the parse.
    let ctx = MemoryContext::new("process_postgres_switches.parse_long_option");
    let (name, value) = guc_seams::parse_long_option::call(ctx.mcx(), string)?;
    Ok((
        name.as_str().to_owned(),
        value.map(|v| v.as_str().to_owned()),
    ))
}

/// `atoi(s)` — leading-integer parse, returning `0` for non-numeric (C `atoi`).
fn atoi(s: &str) -> i32 {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut end = 0;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        end = 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    s[..end].parse::<i32>().unwrap_or(0)
}

/// Leak an owned `String` to `&'static str`, mirroring the C `strdup` whose
/// result lives for the process lifetime (`userDoption` is never freed).
fn leak(s: alloc::string::String) -> &'static str {
    alloc::boxed::Box::leak(s.into_boxed_str())
}

// ---- GUC check / assign hooks (postgres.c:3543-3669) ----

/// `check_client_connection_check_interval(int *newval, void **extra, GucSource
/// source)` (postgres.c:3543).
fn check_client_connection_check_interval(
    newval: &mut i32,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    // WaitEventSetCanReportClosed() is true on the platforms this port targets
    // (epoll/kqueue), so the C `!WaitEventSetCanReportClosed() && *newval != 0`
    // rejection never triggers here.
    if !wait_event_set_can_report_closed() && *newval != 0 {
        guc_check_errdetail(
            "\"client_connection_check_interval\" must be set to 0 on this platform.",
        );
        return Ok(false);
    }
    Ok(true)
}

/// `WaitEventSetCanReportClosed()` (latch.c) — true on poll backends that can
/// report POLLRDHUP (epoll/kqueue). Targeted platforms always can.
fn wait_event_set_can_report_closed() -> bool {
    cfg!(any(target_os = "linux", target_os = "macos"))
}

/// `check_stage_log_stats(bool *newval, void **extra, GucSource source)`
/// (postgres.c:3564).
fn check_stage_log_stats(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if *newval && vars::log_statement_stats.read() {
        guc_check_errdetail("Cannot enable parameter when \"log_statement_stats\" is true.");
        return Ok(false);
    }
    Ok(true)
}

/// `check_log_stats(bool *newval, void **extra, GucSource source)`
/// (postgres.c:3578).
fn check_log_stats(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if *newval
        && (vars::log_parser_stats.read()
            || vars::log_planner_stats.read()
            || vars::log_executor_stats.read())
    {
        guc_check_errdetail(
            "Cannot enable \"log_statement_stats\" when \
             \"log_parser_stats\", \"log_planner_stats\", \
             or \"log_executor_stats\" is true.",
        );
        return Ok(false);
    }
    Ok(true)
}

/// `assign_transaction_timeout(int newval, void *extra)` (postgres.c:3593).
fn assign_transaction_timeout(newval: i32, _extra: Option<&GucHookExtra>) {
    if transam_xact_seams::is_transaction_state::call() {
        // If transaction_timeout GUC has changed within the transaction block
        // enable or disable the timer correspondingly.
        if newval > 0
            && !misc_timeout::get_timeout_active(types_timeout::TimeoutId::TRANSACTION_TIMEOUT)
        {
            // The C ignores enable_timeout_after's error surface (the assign hook
            // is `void`); a failure here would be a wiring bug.
            let _ = misc_timeout::enable_timeout_after(
                types_timeout::TimeoutId::TRANSACTION_TIMEOUT,
                newval,
            );
        } else if newval <= 0
            && misc_timeout::get_timeout_active(types_timeout::TimeoutId::TRANSACTION_TIMEOUT)
        {
            misc_timeout::disable_timeout(
                types_timeout::TimeoutId::TRANSACTION_TIMEOUT,
                false,
            );
        }
    }
}

/// `check_restrict_nonsystem_relation_kind(char **newval, void **extra,
/// GucSource source)` (postgres.c:3612).
fn check_restrict_nonsystem_relation_kind(
    newval: &mut Option<alloc::string::String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let rawstring = newval.clone().unwrap_or_default();
    let elemlist = match split_identifier_string(&rawstring) {
        Some(list) => list,
        None => {
            // syntax error in list
            guc_check_errdetail("List syntax is invalid.");
            return Ok(false);
        }
    };

    let mut flags = 0;
    for tok in &elemlist {
        if pgstrcasecmp::pg_strcasecmp(tok.as_bytes(), b"view") == 0 {
            flags |= RESTRICT_RELKIND_VIEW;
        } else if pgstrcasecmp::pg_strcasecmp(tok.as_bytes(), b"foreign-table") == 0 {
            flags |= RESTRICT_RELKIND_FOREIGN_TABLE;
        } else {
            guc_check_errdetail(&alloc::format!("Unrecognized key word: \"{tok}\"."));
            return Ok(false);
        }
    }

    // Save the flags in *extra, for use by the assign function.
    *extra = Some(alloc::boxed::Box::new(flags));
    Ok(true)
}

/// `assign_restrict_nonsystem_relation_kind(const char *newval, void *extra)`
/// (postgres.c:3664).
fn assign_restrict_nonsystem_relation_kind(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    if let Some(extra) = extra {
        if let Some(flags) = extra.downcast_ref::<i32>() {
            set_restrict_nonsystem_relation_kind(*flags);
        }
    }
}

/// `GUC_check_errdetail(...)` (guc.h).
fn guc_check_errdetail(detail: &str) {
    guc_seams::guc_check_errdetail::call(detail.to_string());
}

/// `SplitIdentifierString(rawstring, ',', &elemlist)` — `Some(tokens)` on
/// success, `None` on a list-syntax error.
fn split_identifier_string(rawstring: &str) -> Option<Vec<alloc::string::String>> {
    user_seams::split_identifier_string::call(rawstring.to_string())
        .ok()
        .flatten()
}

/// Install the GUC check/assign hook fns owned by postgres.c into the GUC
/// tables' typed slots, and the `restrict_nonsystem_relation_kind` reader seam.
pub fn install_guc_hooks() {
    use guc_tables::hooks;
    hooks::check_client_connection_check_interval.install(check_client_connection_check_interval);
    hooks::check_stage_log_stats.install(check_stage_log_stats);
    hooks::check_log_stats.install(check_log_stats);
    hooks::assign_transaction_timeout.install(assign_transaction_timeout);
    hooks::check_restrict_nonsystem_relation_kind.install(check_restrict_nonsystem_relation_kind);
    hooks::assign_restrict_nonsystem_relation_kind.install(assign_restrict_nonsystem_relation_kind);

    // The GUC's own `conf->variable` is the raw string
    // `restrict_nonsystem_relation_kind_string` (postgres.c); install its slot
    // over the string backing cell. The check/assign hooks above turn the
    // string into the parsed `restrict_nonsystem_relation_kind` bitmask.
    use guc_tables::{vars, GucVarAccessors};
    vars::restrict_nonsystem_relation_kind_string.install(GucVarAccessors {
        get: restrict_nonsystem_relation_kind_string,
        set: set_restrict_nonsystem_relation_kind_string,
    });
}

// ===========================================================================
// `getopt(3)` state machine — `process_postgres_switches` parses with the libc
// `getopt(argc, argv, "B:bC:c:D:d:EeFf:h:ijk:lN:nOPp:r:S:sTt:v:W:-:")`. That is
// pure argument-string logic (no syscall), ported in-crate as a faithful
// `getopt` covering the option forms this optstring uses (short option clusters,
// `-x value` / `-xvalue`, and the special `-:` long-option capture). `'?'` is
// the C `default:` (unknown option).
// ===========================================================================

/// A minimal `getopt` state machine over an owned argv slice.
struct Getopt {
    argv: Vec<String>,
    optstring: Vec<u8>,
    /// `optind` — index of the next argv element to scan.
    optind: usize,
    /// offset within the current cluster of short options (`-abc`).
    place: usize,
    /// `optarg` — the option's argument, if it took one.
    optarg: Option<String>,
}

impl Getopt {
    fn new(argv: &[String], optstring: &str) -> Self {
        Getopt {
            argv: argv.to_vec(),
            optstring: optstring.as_bytes().to_vec(),
            optind: 1, // getopt starts scanning at argv[1]
            place: 0,
            optarg: None,
        }
    }

    fn argc(&self) -> usize {
        self.argv.len()
    }

    fn argv_at(&self, idx: usize) -> &str {
        self.argv.get(idx).map(String::as_str).unwrap_or("")
    }

    /// `optarg`, as a `String` (empty if the option took no argument). The
    /// optstring used here only has argument-taking options call this.
    fn optarg_str(&self) -> String {
        self.optarg.clone().unwrap_or_default()
    }

    /// Look up `c` in the optstring; returns `(found, takes_arg)`.
    fn lookup(&self, c: u8) -> (bool, bool) {
        let mut i = 0;
        while i < self.optstring.len() {
            if self.optstring[i] == c {
                let takes_arg = self.optstring.get(i + 1).copied() == Some(b':');
                return (true, takes_arg);
            }
            i += 1;
        }
        (false, false)
    }

    /// `getopt(argc, argv, optstring)` — return the next option char, `None` at
    /// end of options. `'?'` is returned for an unknown option (C `default:`).
    fn next(&mut self) -> Option<char> {
        self.optarg = None;

        if self.place == 0 {
            if self.optind >= self.argv.len() {
                return None;
            }
            let token = &self.argv[self.optind];
            let bytes = token.as_bytes();
            if bytes.is_empty() || bytes[0] != b'-' || bytes.len() == 1 {
                return None;
            }
            if token == "--" {
                self.optind += 1;
                return None;
            }
            self.place = 1;
        }

        let token = self.argv[self.optind].clone();
        let bytes = token.as_bytes();
        let c = bytes[self.place];
        self.place += 1;

        // the '-' option (from "-:") takes the remainder as its argument.
        if c == b'-' && self.lookup(b'-').0 {
            let rest = String::from(&token[self.place..]);
            self.optarg = Some(rest);
            self.place = 0;
            self.optind += 1;
            return Some('-');
        }

        let (found, takes_arg) = self.lookup(c);

        if !found {
            if self.place >= bytes.len() {
                self.place = 0;
                self.optind += 1;
            }
            return Some('?');
        }

        if takes_arg {
            if self.place < bytes.len() {
                self.optarg = Some(String::from(&token[self.place..]));
                self.place = 0;
                self.optind += 1;
            } else {
                self.optind += 1;
                if self.optind < self.argv.len() {
                    self.optarg = Some(self.argv[self.optind].clone());
                    self.optind += 1;
                } else {
                    self.optarg = None;
                }
                self.place = 0;
            }
        } else if self.place >= bytes.len() {
            self.place = 0;
            self.optind += 1;
        }

        Some(c as char)
    }
}
