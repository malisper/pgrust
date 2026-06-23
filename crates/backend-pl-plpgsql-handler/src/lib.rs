//! `backend-pl-plpgsql-handler` — the PL/pgSQL fmgr-facing handler
//! (`pl_handler.c`), the top (7th) layer of the PL/pgSQL subsystem.
//!
//! Ports the three `PG_FUNCTION_INFO_V1` entry points the function manager and
//! `ExecuteDoStmt` dispatch to — [`plpgsql_call_handler`] (LANGUAGE plpgsql
//! function/procedure calls), [`plpgsql_inline_handler`] (`DO` blocks), and
//! [`plpgsql_validator`] (CREATE FUNCTION-time body checking) — plus the
//! module-load initialization [`_pg_init`] (custom GUCs, the xact/subxact
//! callback registration, the plugin rendezvous) and the two GUC check/assign
//! hooks for `plpgsql.extra_warnings` / `plpgsql.extra_errors`.
//!
//! [`init_seams`] installs `execute_inline_handler`
//! (`backend-commands-functioncmds-seams`) — the DO-block dispatch the
//! functioncmds `ExecuteDoStmt` reaches — and registers the three handler
//! `PGFunction`s in the fmgr built-in registry by name, so a C-language
//! `pg_proc` row (`prosrc = "plpgsql_call_handler"` / …) resolves here.
//!
//! ## What runs vs. what is loud
//!
//! A `DO`/function body of pure control flow (empty block, IF/CASE/LOOP with no
//! SQL or expression evaluation that reaches the value substrate) runs
//! end-to-end: handler → `plpgsql_compile_inline` → `plpgsql_exec_function` →
//! `exec_toplevel_block`. Any leg that evaluates a SQL expression, runs a query
//! through SPI, or assigns a computed value bottoms out in the exec value
//! substrate (the executor `ExprState` simple-expr path / the SPI plan surface)
//! and panics loudly there — faithful: a C build would reach exactly those
//! callees. `_pg_init`'s GUC-registration and xact-callback-registration are
//! likewise routed to loud seams (the custom-GUC `DefineCustom*Variable`
//! substrate and `plpgsql_xact_cb`/`_subxact_cb` — defined in `pl_exec.c` — are
//! not yet ported); the compile-time GUC defaults already match the C source
//! defaults, so the compiler reads correct values without `_pg_init`.

#![allow(non_camel_case_types, non_snake_case)]

mod seam;

use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::fmgr::FunctionCallInfoBaseData;
use types_fmgr::BuiltinFunction;
use types_parsenodes::InlineCodeBlock;
use types_plpgsql::{
    int32, PLpgSQL_resolve_option, PLPGSQL_XCHECK_ALL, PLPGSQL_XCHECK_NONE,
    PLPGSQL_XCHECK_SHADOWVAR, PLPGSQL_XCHECK_STRICTMULTIASSIGNMENT, PLPGSQL_XCHECK_TOOMANYROWS,
};

use backend_executor_spi::{SPI_connect_ext, SPI_finish, SPI_OK_FINISH, SPI_OPT_NONATOMIC};
use backend_pl_plpgsql_exec::{FunctionCallArg, FunctionResult};

// ---------------------------------------------------------------------------
// Custom GUC variables (pl_handler.c module globals).
//
// In C these are pl_handler.c process globals the GUC machinery populates and
// `pl_comp.c` reads. In the owned model the compiler crate
// (backend-pl-plpgsql-comp) holds the same globals privately, seeded with the
// identical C-source defaults (PLPGSQL_RESOLVE_ERROR / false / true / 0 / 0);
// the assign hooks below write into the handler-local mirrors used for
// `_pg_init` / fmgr-side reads. Both stay in sync via the assign hooks.
// ---------------------------------------------------------------------------

use core::cell::{Cell, OnceCell, RefCell};
use types_datum::VARHDRSZ;

thread_local! {
    /// Backend-lifetime context for the EXCEPTION handler's SQLSTATE/SQLERRM
    /// (and GET DIAGNOSTICS) `text` values: `CStringGetTextDatum` allocations
    /// that outlive the producing call because they are stored into a PL/pgSQL
    /// variable. Leaked once (never dropped), mirroring C's palloc lifetime.
    static PLPGSQL_ERRVAR_CONTEXT: OnceCell<&'static mcx::MemoryContext> =
        const { OnceCell::new() };
    /// `int plpgsql_variable_conflict = PLPGSQL_RESOLVE_ERROR;`
    static PLPGSQL_VARIABLE_CONFLICT: Cell<PLpgSQL_resolve_option> =
        const { Cell::new(PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR) };
    /// `bool plpgsql_print_strict_params = false;`
    static PLPGSQL_PRINT_STRICT_PARAMS: Cell<bool> = const { Cell::new(false) };
    /// `bool plpgsql_check_asserts = true;`
    static PLPGSQL_CHECK_ASSERTS: Cell<bool> = const { Cell::new(true) };
    /// `int plpgsql_extra_warnings;` (default "none" => PLPGSQL_XCHECK_NONE)
    static PLPGSQL_EXTRA_WARNINGS: Cell<int32> = const { Cell::new(PLPGSQL_XCHECK_NONE) };
    /// `int plpgsql_extra_errors;` (default "none" => PLPGSQL_XCHECK_NONE)
    static PLPGSQL_EXTRA_ERRORS: Cell<int32> = const { Cell::new(PLPGSQL_XCHECK_NONE) };
    /// `static char *plpgsql_extra_warnings_string = NULL;` — the GUC's string
    /// storage (`config_string.variable`). The assign hook derives the bitmask
    /// `plpgsql_extra_warnings` from it; this cell holds the textual value SHOW /
    /// current_setting render. Boot value "none" is seeded at registration.
    static PLPGSQL_EXTRA_WARNINGS_STRING: RefCell<Option<String>> =
        const { RefCell::new(None) };
    /// `static char *plpgsql_extra_errors_string = NULL;`.
    static PLPGSQL_EXTRA_ERRORS_STRING: RefCell<Option<String>> =
        const { RefCell::new(None) };
}

/// Read `plpgsql_variable_conflict`.
pub fn plpgsql_variable_conflict() -> PLpgSQL_resolve_option {
    PLPGSQL_VARIABLE_CONFLICT.with(Cell::get)
}
/// Read `plpgsql_print_strict_params`.
pub fn plpgsql_print_strict_params() -> bool {
    PLPGSQL_PRINT_STRICT_PARAMS.with(Cell::get)
}
/// Read `plpgsql_check_asserts`.
pub fn plpgsql_check_asserts() -> bool {
    PLPGSQL_CHECK_ASSERTS.with(Cell::get)
}
/// Read `plpgsql_extra_warnings`.
pub fn plpgsql_extra_warnings() -> int32 {
    PLPGSQL_EXTRA_WARNINGS.with(Cell::get)
}
/// Read `plpgsql_extra_errors`.
pub fn plpgsql_extra_errors() -> int32 {
    PLPGSQL_EXTRA_ERRORS.with(Cell::get)
}

// ---------------------------------------------------------------------------
// GUC storage accessors (`config_*.variable`): the get/set pairs the custom-GUC
// machinery writes through when a `SET plpgsql.x` is applied. Each mirrors C's
// address-shared `*conf->variable = newval`.
// ---------------------------------------------------------------------------

/// `&plpgsql_variable_conflict` get accessor (enum encoded as its i32 repr).
fn get_variable_conflict() -> int32 {
    PLPGSQL_VARIABLE_CONFLICT.with(Cell::get) as int32
}
/// `&plpgsql_variable_conflict` set accessor. In C there is a single
/// `plpgsql_variable_conflict` global (pl_handler.c) that pl_comp.c reads; in the
/// owned model the compiler holds its own per-backend copy, so the assign writes
/// both the handler mirror and the compiler's authoritative cell.
fn set_variable_conflict(v: int32) {
    let opt = match v {
        0 => PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR,
        1 => PLpgSQL_resolve_option::PLPGSQL_RESOLVE_VARIABLE,
        2 => PLpgSQL_resolve_option::PLPGSQL_RESOLVE_COLUMN,
        // The enum GUC only ever produces a valid option index (the parser maps
        // a name to the entry's `val`), so other values cannot arrive.
        _ => PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR,
    };
    PLPGSQL_VARIABLE_CONFLICT.with(|c| c.set(opt));
    backend_pl_plpgsql_comp::set_plpgsql_variable_conflict(opt);
}

/// `&plpgsql_print_strict_params` get/set.
fn get_print_strict_params() -> bool {
    PLPGSQL_PRINT_STRICT_PARAMS.with(Cell::get)
}
fn set_print_strict_params(v: bool) {
    PLPGSQL_PRINT_STRICT_PARAMS.with(|c| c.set(v));
    backend_pl_plpgsql_comp::set_plpgsql_print_strict_params(v);
}

/// `&plpgsql_check_asserts` get/set.
fn get_check_asserts() -> bool {
    PLPGSQL_CHECK_ASSERTS.with(Cell::get)
}
fn set_check_asserts(v: bool) {
    // The exec-seam `plpgsql_check_asserts` is a function-pointer seam installed
    // once at boot (`init_seams`) that reads this live cell, so updating the cell
    // is enough — `exec_stmt_assert` sees the new value without re-installing.
    PLPGSQL_CHECK_ASSERTS.with(|c| c.set(v));
}

/// `&plpgsql_extra_warnings_string` get/set (the string GUC's storage).
fn get_extra_warnings_string() -> Option<String> {
    PLPGSQL_EXTRA_WARNINGS_STRING.with(|c| c.borrow().clone())
}
fn set_extra_warnings_string(v: Option<String>) {
    PLPGSQL_EXTRA_WARNINGS_STRING.with(|c| *c.borrow_mut() = v);
}

/// `&plpgsql_extra_errors_string` get/set.
fn get_extra_errors_string() -> Option<String> {
    PLPGSQL_EXTRA_ERRORS_STRING.with(|c| c.borrow().clone())
}
fn set_extra_errors_string(v: Option<String>) {
    PLPGSQL_EXTRA_ERRORS_STRING.with(|c| *c.borrow_mut() = v);
}

// ---------------------------------------------------------------------------
// GUC check / assign hooks for plpgsql.extra_warnings / plpgsql.extra_errors.
// ---------------------------------------------------------------------------

/// `plpgsql_extra_checks_check_hook(char **newvalue, void **extra, GucSource)`
/// (pl_handler.c) — the GUC *check* hook shared by `plpgsql.extra_warnings` and
/// `plpgsql.extra_errors`. Parses the comma-separated identifier list into the
/// `PLPGSQL_XCHECK_*` bitmask the assign hook will store.
///
/// Returns `Ok(bitmask)` for a valid value, or `Err(detail)` (the C
/// `GUC_check_errdetail` text) on a list-syntax / unrecognized-keyword error.
/// `mcx` backs the `SplitIdentifierString` scratch parse.
pub fn plpgsql_extra_checks_check_hook(
    mcx: mcx::Mcx<'_>,
    newvalue: &str,
) -> PgResult<Result<int32, String>> {
    let mut extrachecks: int32 = 0;

    if newvalue.eq_ignore_ascii_case("all") {
        extrachecks = PLPGSQL_XCHECK_ALL;
    } else if newvalue.eq_ignore_ascii_case("none") {
        extrachecks = PLPGSQL_XCHECK_NONE;
    } else {
        // Parse string into list of identifiers (C: pstrdup + SplitIdentifierString).
        let elemlist = match backend_utils_adt_varlena::split_format::split_identifier_string(
            mcx, newvalue, ',',
        )? {
            Some(list) => list,
            None => return Ok(Err(String::from("List syntax is invalid."))),
        };

        for tok in elemlist.iter() {
            let tok = tok.as_str();
            if tok.eq_ignore_ascii_case("shadowed_variables") {
                extrachecks |= PLPGSQL_XCHECK_SHADOWVAR;
            } else if tok.eq_ignore_ascii_case("too_many_rows") {
                extrachecks |= PLPGSQL_XCHECK_TOOMANYROWS;
            } else if tok.eq_ignore_ascii_case("strict_multi_assignment") {
                extrachecks |= PLPGSQL_XCHECK_STRICTMULTIASSIGNMENT;
            } else if tok.eq_ignore_ascii_case("all") || tok.eq_ignore_ascii_case("none") {
                return Ok(Err(format!(
                    "Key word \"{tok}\" cannot be combined with other key words."
                )));
            } else {
                return Ok(Err(format!("Unrecognized key word: \"{tok}\".")));
            }
        }
    }

    Ok(Ok(extrachecks))
}

/// `plpgsql_extra_warnings_assign_hook(const char *newvalue, void *extra)`
/// (pl_handler.c) — store the checked bitmask into `plpgsql_extra_warnings`. The
/// compiler reads its own per-backend copy of this global, so propagate there too.
pub fn plpgsql_extra_warnings_assign_hook(extra: int32) {
    PLPGSQL_EXTRA_WARNINGS.with(|c| c.set(extra));
    backend_pl_plpgsql_comp::set_plpgsql_extra_warnings(extra);
}

/// `plpgsql_extra_errors_assign_hook(const char *newvalue, void *extra)`
/// (pl_handler.c) — store the checked bitmask into `plpgsql_extra_errors`.
pub fn plpgsql_extra_errors_assign_hook(extra: int32) {
    PLPGSQL_EXTRA_ERRORS.with(|c| c.set(extra));
    backend_pl_plpgsql_comp::set_plpgsql_extra_errors(extra);
}

// ---------------------------------------------------------------------------
// GUC-shaped check / assign hooks (the `GucStringCheckFn` / `GucStringAssignFn`
// the custom-GUC machinery calls). The shared check hook parses the list into
// the `PLPGSQL_XCHECK_*` bitmask and stashes it as the `extra` payload (C's
// `*extra = malloc(int)`); the per-variable assign hook reads the bitmask back.
// ---------------------------------------------------------------------------

/// The `extra` payload `plpgsql_extra_checks_check_hook` produces — the parsed
/// `PLPGSQL_XCHECK_*` bitmask (C's `int *extra`).
struct ExtraChecksBitmask(int32);

/// `GucStringCheckFn` for `plpgsql.extra_warnings` / `plpgsql.extra_errors`:
/// validate the comma-separated keyword list and produce the bitmask `extra`.
fn extra_checks_guc_check_hook(
    newval: &mut Option<String>,
    extra: &mut Option<backend_utils_misc_guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    // A NULL value (the C `newval == NULL`) cannot occur for these GUCs (boot
    // value "none"); treat it as "none".
    let value = newval.as_deref().unwrap_or("none");
    let scratch = mcx::MemoryContext::new("plpgsql_extra_checks_check_hook");
    match plpgsql_extra_checks_check_hook(scratch.mcx(), value)? {
        Ok(bitmask) => {
            *extra = Some(Box::new(ExtraChecksBitmask(bitmask)));
            Ok(true)
        }
        Err(detail) => {
            // C: GUC_check_errdetail("%s", detail) + return false. Surface the
            // detail through the check-error channel the GUC layer reads.
            backend_utils_misc_guc::GUC_check_errdetail(detail);
            Ok(false)
        }
    }
}

/// `GucStringAssignFn` for `plpgsql.extra_warnings`.
fn extra_warnings_guc_assign_hook(
    _newval: Option<&str>,
    extra: Option<&backend_utils_misc_guc_tables::GucHookExtra>,
) {
    if let Some(b) = extra.and_then(|e| e.downcast_ref::<ExtraChecksBitmask>()) {
        plpgsql_extra_warnings_assign_hook(b.0);
    }
}

/// `GucStringAssignFn` for `plpgsql.extra_errors`.
fn extra_errors_guc_assign_hook(
    _newval: Option<&str>,
    extra: Option<&backend_utils_misc_guc_tables::GucHookExtra>,
) {
    if let Some(b) = extra.and_then(|e| e.downcast_ref::<ExtraChecksBitmask>()) {
        plpgsql_extra_errors_assign_hook(b.0);
    }
}

// ---------------------------------------------------------------------------
// _PG_init — library load-time initialization (pl_handler.c).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static bool inited = false;` inside `_PG_init`.
    static PG_INIT_INITED: Cell<bool> = const { Cell::new(false) };
    /// Whether [`register_custom_gucs`] has run for this backend (the custom-GUC
    /// slice of `_PG_init`, fired lazily on first plpgsql library use).
    static CUSTOM_GUCS_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

/// `_PG_init`'s custom-GUC registration, fired once per backend on first PL/pgSQL
/// library use (the call handler / inline handler / validator entry points). C
/// runs the whole `_PG_init` at library load; here only the GUC-registration
/// slice is reached (the xact-callback / rendezvous slices remain loud seams).
/// Idempotent.
fn ensure_custom_gucs_registered() {
    if CUSTOM_GUCS_REGISTERED.with(Cell::get) {
        return;
    }
    register_custom_gucs();
    CUSTOM_GUCS_REGISTERED.with(|c| c.set(true));
}

/// `_PG_init(void)` (pl_handler.c) — library load-time initialization.
///
/// Defines the custom GUCs, marks the `plpgsql` prefix reserved, registers the
/// transaction/subtransaction callbacks, and sets up the plugin rendezvous. The
/// custom-GUC registration (`DefineCustom*Variable` / `MarkGUCPrefixReserved`),
/// the xact-callback registration (`RegisterXactCallback(plpgsql_xact_cb)` /
/// `RegisterSubXactCallback(plpgsql_subxact_cb)` — those callbacks live in
/// `pl_exec.c` and are not yet ported), and the rendezvous lookup route through
/// [`seam`] (loud until that substrate lands). The compile-time GUC defaults
/// already match the C source, so the compiler reads correct values even before
/// `_pg_init` fires.
pub fn _pg_init() {
    // Be sure we do initialization only once (should be redundant now).
    if PG_INIT_INITED.with(Cell::get) {
        return;
    }

    seam::pg_bindtextdomain();

    register_custom_gucs();

    // RegisterXactCallback(plpgsql_xact_cb, NULL);
    seam::register_xact_callback();
    // RegisterSubXactCallback(plpgsql_subxact_cb, NULL);
    seam::register_subxact_callback();

    // Set up a rendezvous point with optional instrumentation plugin.
    seam::find_rendezvous_variable_plpgsql_plugin();

    PG_INIT_INITED.with(|c| c.set(true));
}

/// `static const struct config_enum_entry variable_conflict_options[]`
/// (pl_handler.c).
static VARIABLE_CONFLICT_OPTIONS: &[types_guc::config_enum_entry] = &[
    types_guc::config_enum_entry { name: "error", val: 0, hidden: false },
    types_guc::config_enum_entry { name: "use_variable", val: 1, hidden: false },
    types_guc::config_enum_entry { name: "use_column", val: 2, hidden: false },
];

/// The custom-GUC registration block of `_PG_init` (the five
/// `DefineCustom*Variable` + `MarkGUCPrefixReserved`). Split out so seam
/// installation can register the GUCs once the GUC store is up, without firing
/// the still-loud xact-callback / rendezvous seams. Idempotent via
/// [`PG_INIT_INITED`]-adjacent guard in the caller.
pub fn register_custom_gucs() {
    use backend_utils_misc_guc::custom;
    use backend_utils_misc_guc_tables::GucVarAccessors;
    use types_guc::{GUC_LIST_INPUT, PGC_SUSET, PGC_USERSET};

    // DefineCustomEnumVariable("plpgsql.variable_conflict", …)
    let _ = custom::define_custom_enum_variable(
        "plpgsql.variable_conflict",
        Some("Sets handling of conflicts between PL/pgSQL variable names and table column names."),
        None,
        GucVarAccessors { get: get_variable_conflict, set: set_variable_conflict },
        PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR as int32,
        VARIABLE_CONFLICT_OPTIONS,
        PGC_SUSET,
        0,
        None,
        None,
        None,
    );

    // DefineCustomBoolVariable("plpgsql.print_strict_params", …)
    let _ = custom::define_custom_bool_variable(
        "plpgsql.print_strict_params",
        Some("Print information about parameters in the DETAIL part of the error messages generated on INTO ... STRICT failures."),
        None,
        GucVarAccessors { get: get_print_strict_params, set: set_print_strict_params },
        false,
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    // DefineCustomBoolVariable("plpgsql.check_asserts", …)
    let _ = custom::define_custom_bool_variable(
        "plpgsql.check_asserts",
        Some("Perform checks given in ASSERT statements."),
        None,
        GucVarAccessors { get: get_check_asserts, set: set_check_asserts },
        true,
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    // DefineCustomStringVariable("plpgsql.extra_warnings", …, check/assign hooks)
    let _ = custom::define_custom_string_variable(
        "plpgsql.extra_warnings",
        Some("List of programming constructs that should produce a warning."),
        None,
        GucVarAccessors { get: get_extra_warnings_string, set: set_extra_warnings_string },
        Some("none"),
        PGC_USERSET,
        GUC_LIST_INPUT,
        Some(extra_checks_guc_check_hook),
        Some(extra_warnings_guc_assign_hook),
        None,
    );

    // DefineCustomStringVariable("plpgsql.extra_errors", …, check/assign hooks)
    let _ = custom::define_custom_string_variable(
        "plpgsql.extra_errors",
        Some("List of programming constructs that should produce an error."),
        None,
        GucVarAccessors { get: get_extra_errors_string, set: set_extra_errors_string },
        Some("none"),
        PGC_USERSET,
        GUC_LIST_INPUT,
        Some(extra_checks_guc_check_hook),
        Some(extra_errors_guc_assign_hook),
        None,
    );

    custom::mark_guc_prefix_reserved("plpgsql");
}

// ---------------------------------------------------------------------------
// plpgsql_call_handler — fmgr call handler (LANGUAGE plpgsql functions).
// ---------------------------------------------------------------------------

/// `plpgsql_call_handler(fcinfo)` (pl_handler.c) — the call handler the function
/// manager and trigger manager invoke to execute a compiled PL/pgSQL function
/// or procedure.
///
/// Connects to SPI (nonatomic iff a `CallContext` says so), compiles-or-finds
/// the function, marks it busy, creates the procedure resowner if needed, runs
/// the appropriate sub-handler under PG_TRY/PG_FINALLY (use-count decrement +
/// cur_estate restore + resowner release), and disconnects SPI.
///
/// The trigger / event-trigger dispatch (`plpgsql_exec_trigger` /
/// `plpgsql_exec_event_trigger`) routes to the loud trigger entries in exec; the
/// scalar/procedure path runs `plpgsql_exec_function`. `plpgsql_compile` reads
/// the live `FunctionCallInfo` + catalog (the compile entry is owned by comp).
/// Flatten a by-reference fmgr argument/value payload (`fcinfo.ref_arg(i)`) to
/// its verbatim owned byte image. A `Varlena`/`Composite` arm is the header-ful
/// flat image; a `Cstring` is the NUL-excluded text bytes. The `Expanded` /
/// `Internal` arms are never a PL/pgSQL scalar argument/value (no flat image),
/// so they degrade to `None` (treated as by-value — C never reaches them here).
fn ref_payload_image(p: Option<&types_fmgr::boundary::RefPayload>) -> Option<std::vec::Vec<u8>> {
    use types_fmgr::boundary::RefPayload;
    match p {
        Some(RefPayload::Varlena(b)) => Some(b.clone()),
        Some(RefPayload::Composite(b)) => Some(b.clone()),
        Some(RefPayload::Cstring(s)) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

pub fn plpgsql_call_handler(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // _PG_init's custom-GUC registration (fired on first library use).
    ensure_custom_gucs_registered();
    // nonatomic = fcinfo->context is a CallContext with atomic == false.
    let nonatomic = seam::called_nonatomic(fcinfo);

    // Connect to SPI manager.
    let opts = if nonatomic { SPI_OPT_NONATOMIC } else { 0 };
    let _ = SPI_connect_ext(opts);

    // Run the dispatch in a closure so the SPI_finish below runs on BOTH the
    // Ok and the Err path (C's PG_FINALLY: the SPI bracket must close, and the
    // use-count decrement / cur_estate restore happen, even when the PL body
    // raised). The body returns the marshaled `Datum` (a by-reference result is
    // deposited on `fcinfo`'s ref-result side-channel inside).
    let body = (|| -> PgResult<Datum> {
        // Find or compile the function. `plpgsql_compile(fcinfo, false)` (comp)
        // takes the live arena-lifetimed `types_nodes` fcinfo; the fmgr
        // `PGFunction` boundary carries the non-lifetimed `types_fmgr` fcinfo, so
        // the fcinfo-model bridge is the fmgr-dispatch substrate.
        let func = seam::compile_for_call(fcinfo);

        // A pass-by-reference argument (`text`/`varchar`/`numeric`/…) crosses the
        // fmgr boundary as an owned image in the parallel `ref_args` side-channel
        // (C's "`args[i].value` is a pointer to `payload`"). Carry the verbatim
        // header-ful varlena / cstring bytes alongside the bare-word `value` so
        // the arg-store leg can keep the image in the local variable; a by-value
        // argument has no `ref_args` entry (`byref == None`).
        let args: Vec<FunctionCallArg> = fcinfo
            .args
            .iter()
            .enumerate()
            .map(|(i, a)| FunctionCallArg {
                value: a.value,
                isnull: a.isnull,
                byref: ref_payload_image(fcinfo.ref_arg(i)),
            })
            .collect();

        let procedure_resowner = if nonatomic && func.requires_procedure_resowner {
            Some(seam::create_procedure_resowner())
        } else {
            None
        };

        // The PL executor is now Result-native end to end: a SQL error in the
        // body arrives as `Err(PgError)` (via `?`) instead of a `panic_any`, and
        // is propagated straight to the fmgr boundary below.
        let result: FunctionResult = if seam::called_as_trigger(fcinfo) {
            // PointerGetDatum(plpgsql_exec_trigger(func, trigdata))
            let trigdata = seam::take_trigger_data(fcinfo);
            let d = backend_pl_plpgsql_exec::plpgsql_exec_trigger(&func, trigdata)?;
            FunctionResult {
                value: d,
                isnull: false,
                byref: None,
                rettype: 0,
            }
        } else if seam::called_as_event_trigger(fcinfo) {
            let trigdata = seam::take_event_trigger_data(fcinfo);
            backend_pl_plpgsql_exec::plpgsql_exec_event_trigger(&func, trigdata)?;
            // no return value in this case
            FunctionResult {
                value: Datum::null(),
                isnull: false,
                byref: None,
                rettype: 0,
            }
        } else {
            backend_pl_plpgsql_exec::plpgsql_exec_function(
                &func,
                &args,
                None,
                None,
                procedure_resowner,
                !nonatomic,
            )?
        };

        fcinfo.isnull = result.isnull;

        // A pass-by-reference result (text/varchar/numeric/SQLERRM/…) crosses the
        // fmgr boundary through `ref_result`: set the owned varlena image
        // (already header-ful, `datumCopy`'d out of the exec/SPI context, so it
        // outlives the SPI bracket we close just below) and return the dummy
        // word. fmgr-core's result marshaling (`take_ref_result` → `FmgrOut::Ref`)
        // reconstructs the value. A by-value result returns its scalar word
        // directly (`byref == None`).
        let ret = match result.byref {
            Some(image) if !result.isnull => {
                fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(image));
                Datum::from_usize(0)
            }
            _ => result.value,
        };
        Ok(ret)
    })();

    // PG_FINALLY: disconnect from SPI manager on both paths. On the error path,
    // close SPI and then propagate the original body error (a SPI_finish failure
    // there is secondary and would mask the real error, so the body error wins).
    let finish = SPI_finish();
    match &body {
        Ok(_) => match finish {
            Ok(SPI_OK_FINISH) => {}
            Ok(rc) => seam::elog_spi_finish_failed(rc),
            Err(e) => return Err(e.clone()),
        },
        Err(_) => {
            // The body already failed; SPI is being torn down by the surrounding
            // subtransaction abort, so a non-FINISH code / error here is expected
            // and must not mask the body error.
        }
    }

    body
}

// ---------------------------------------------------------------------------
// plpgsql_inline_handler — fmgr handler for anonymous DO code blocks.
// ---------------------------------------------------------------------------

/// `plpgsql_inline_handler(fcinfo)` (pl_handler.c) — execute an anonymous `DO`
/// code block.
///
/// Connects to SPI (nonatomic iff `!codeblock->atomic`), compiles the block via
/// `plpgsql_compile_inline`, sets up a private simple-expression `EState` +
/// resowner (NOT tied to the transaction so they survive any COMMIT/ROLLBACK in
/// the block), runs `plpgsql_exec_function` under PG_TRY/PG_CATCH (the catch
/// path flushes the failed block's long-lived resources), frees the function,
/// and disconnects SPI. Returns `(Datum) 0`.
///
/// This is the native form the `execute_inline_handler` seam installs to. The
/// private-EState/resowner creation + the catch-path resource flush are the
/// SPI/executor substrate; the create/free of the function + exec dispatch is
/// real. A pure-control-flow block runs end-to-end.
pub fn plpgsql_inline_handler(codeblock: InlineCodeBlock) -> PgResult<()> {
    // _PG_init's custom-GUC registration (fired on first library use).
    ensure_custom_gucs_registered();
    // Connect to SPI manager.
    let opts = if codeblock.atomic { 0 } else { SPI_OPT_NONATOMIC };
    let _ = SPI_connect_ext(opts);

    // Compile the anonymous code block.
    let source = codeblock.source_text.clone().unwrap_or_default();
    let mut func = backend_pl_plpgsql_comp::plpgsql_compile_inline(source)?;

    // Mark the function as busy, just pro forma (funccache use_count; substrate).

    // Create a private EState + resowner for simple-expression execution. These
    // are NOT tied to transaction-level resources. (SPI/executor substrate;
    // None until that lands — exec creates its econtext lazily, and a
    // control-flow-only block never reads them.)
    let simple_eval_estate = None;
    let simple_eval_resowner = None;

    // Run the function (fake fcinfo with no args). The PL executor is
    // Result-native; a body SQL error arrives as `Err` and propagates with `?`
    // (the DO-block PG_CATCH resource flush is the SPI/executor substrate, still
    // owned by the surrounding bracket).
    let _result = backend_pl_plpgsql_exec::plpgsql_exec_function(
        &func,
        &[],
        simple_eval_estate,
        simple_eval_resowner,
        None,
        codeblock.atomic,
    )?;

    // Function should now have no remaining use-counts; free subsidiary storage.
    backend_pl_plpgsql_funcs::plpgsql_free_function_memory(&mut func)?;

    // Disconnect from SPI manager.
    match SPI_finish() {
        Ok(SPI_OK_FINISH) => {}
        Ok(rc) => seam::elog_spi_finish_failed(rc),
        Err(e) => return Err(e),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// plpgsql_validator — CREATE FUNCTION-time body validation.
// ---------------------------------------------------------------------------

/// `plpgsql_validator(fcinfo)` (pl_handler.c) — validate a PL/pgSQL function at
/// CREATE FUNCTION time.
///
/// Reads the new function's `pg_proc` row, rejects disallowed pseudotype
/// result/argument types (except TRIGGER/EVTTRIGGER/RECORD/VOID/polymorphic),
/// and — if `check_function_bodies` — test-compiles the body via
/// `plpgsql_compile(forValidator=true)` under an SPI bracket.
///
/// The syscache `pg_proc` read, `get_typtype`, `get_func_arg_info`,
/// `CheckFunctionValidatorAccess`, and the `check_function_bodies` GUC are the
/// catalog/syscache/GUC substrate and route through [`seam`] (loud until they
/// land); the access early-out + the fake-fcinfo test-compile control flow is
/// real.
pub fn plpgsql_validator(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // _PG_init's custom-GUC registration (fired on first library use).
    ensure_custom_gucs_registered();
    let funcoid = seam::getarg_oid(fcinfo, 0);

    if !seam::check_function_validator_access(seam::flinfo_fn_oid(fcinfo), funcoid)? {
        return Ok(Datum::null()); // PG_RETURN_VOID
    }

    // The pg_proc-tuple read, pseudotype-rejection, and the test-compile path
    // all bottom out in the syscache/catalog substrate.
    seam::validate_function_body(funcoid)?;

    Ok(Datum::null()) // PG_RETURN_VOID
}

// ---------------------------------------------------------------------------
// fmgr PGFunction wrappers + builtin registration.
// ---------------------------------------------------------------------------

/// The legacy `PGFunction` ABI wrapper for `plpgsql_call_handler` (the
/// `lookup()` `$libdir/plpgsql` module-resolver path, which hands back a bare
/// `PGFunction`). Re-raises the `Err` the one sanctioned way at the fmgr
/// boundary (`seam::propagate` = `panic_any`); the migrated builtin-registry
/// path uses [`plpgsql_call_handler_native`] and never panics.
fn plpgsql_call_handler_pg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match plpgsql_call_handler(fcinfo) {
        Ok(d) => d,
        Err(e) => seam::propagate(e),
    }
}

/// Result-native form of [`plpgsql_call_handler_pg`] for the migrated builtin
/// registry: a SQL error in the PL body travels back as `Err(PgError)` straight
/// to `invoke_builtin` (no `panic_any`/`catch_unwind`), now that the whole PL
/// executor is Result-threaded.
fn plpgsql_call_handler_native(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    plpgsql_call_handler(fcinfo)
}

/// The `PGFunction` ABI wrapper for `plpgsql_inline_handler`. The
/// `InlineCodeBlock*` arrives as `PG_GETARG_DATUM(0)` (the internal-pointer
/// lane); unpacking it from the fmgr boundary is the INTERNAL-arg substrate.
fn plpgsql_inline_handler_pg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let codeblock = seam::getarg_inline_codeblock(fcinfo);
    match plpgsql_inline_handler(codeblock) {
        Ok(()) => Datum::null(),
        Err(e) => seam::propagate(e),
    }
}

/// The `PGFunction` ABI wrapper for `plpgsql_validator`.
fn plpgsql_validator_pg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match plpgsql_validator(fcinfo) {
        Ok(d) => d,
        Err(e) => seam::propagate(e),
    }
}

/// Result-native form of [`plpgsql_inline_handler_pg`] for the migrated builtin
/// registry: the `Err` travels back as `Err(PgError)` straight to
/// `invoke_builtin` (no `panic_any`/`catch_unwind`). The legacy `*_pg` wrapper is
/// retained for the `lookup()` `PGFunction` module-resolver path.
fn plpgsql_inline_handler_native(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let codeblock = seam::getarg_inline_codeblock(fcinfo);
    plpgsql_inline_handler(codeblock)?;
    Ok(Datum::null())
}

/// Result-native form of [`plpgsql_validator_pg`] for the migrated builtin
/// registry.
fn plpgsql_validator_native(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    plpgsql_validator(fcinfo)
}

/// The simple (suffix-free, directory-free) name of the plpgsql loadable module
/// — `$libdir/plpgsql` reduces to this for the in-process loader registry.
const LIBRARY: &str = "plpgsql";

/// Resolve a symbol of the `plpgsql` module to its ported `PGFunction`, exactly
/// as the OS loader would resolve it in `plpgsql.so`. The three
/// `PG_FUNCTION_INFO_V1` entry points (`plpgsql_call_handler` /
/// `plpgsql_inline_handler` / `plpgsql_validator`) named by the
/// `pg_proc.probin = '$libdir/plpgsql'` rows the extension creates resolve here
/// (api_version 1). Returns `None` for an unknown symbol.
fn lookup(function: &str) -> Option<types_fmgr::LoadedExternalFunc> {
    let user_fn: types_fmgr::fmgr::PGFunction = match function {
        "plpgsql_call_handler" => Some(plpgsql_call_handler_pg),
        "plpgsql_inline_handler" => Some(plpgsql_inline_handler_pg),
        "plpgsql_validator" => Some(plpgsql_validator_pg),
        _ => return None,
    };
    Some(types_fmgr::LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// Register the three handler `PGFunction`s in the fmgr built-in registry by
/// name, so a C-language `pg_proc` row whose `prosrc` is one of these resolves
/// to the Rust function. plpgsql is an extension (`CREATE EXTENSION plpgsql`),
/// so its function OIDs are catalog-assigned at install time, not fixed builtin
/// OIDs; the registry is keyed by name for the C-language lookup
/// (`fmgr_lookup_by_name`). The placeholder `foid = 0` is overwritten by the
/// catalog OID when the row is created.
fn register_handler_builtins() {
    // `plpgsql_call_handler` is now Result-native: the whole PL executor call
    // tree is `PgResult`-threaded, so a SQL error in the body arrives as
    // `Err(PgError)` and dispatches with `?` (no `catch_unwind`). The legacy
    // `*_pg` wrapper is retained only for the `lookup()` `$libdir/plpgsql`
    // module-resolver path (which needs a bare `PGFunction`).
    backend_utils_fmgr_core::register_builtins_native([(
        BuiltinFunction {
            foid: 0,
            name: "plpgsql_call_handler".to_string(),
            nargs: 0,
            strict: false,
            retset: false,
            func: None,
        },
        plpgsql_call_handler_native as types_fmgr::PgFnNative,
    )]);
    // `plpgsql_inline_handler` / `plpgsql_validator` are Result-native: their
    // cores already return `PgResult`, so they register the native callable and
    // dispatch with `?` (no `catch_unwind`).
    backend_utils_fmgr_core::register_builtins_native([
        (
            BuiltinFunction {
                foid: 0,
                name: "plpgsql_inline_handler".to_string(),
                nargs: 1,
                strict: false,
                retset: false,
                func: None,
            },
            plpgsql_inline_handler_native as types_fmgr::PgFnNative,
        ),
        (
            BuiltinFunction {
                foid: 0,
                name: "plpgsql_validator".to_string(),
                nargs: 1,
                strict: false,
                retset: false,
                func: None,
            },
            plpgsql_validator_native as types_fmgr::PgFnNative,
        ),
    ]);
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this crate's seams: the DO-block dispatch (`execute_inline_handler`,
/// reached by functioncmds' `ExecuteDoStmt`) and the three fmgr handler
/// built-ins.
pub fn init_seams() {
    backend_commands_functioncmds_seams::execute_inline_handler::set(|_laninline, codeblock| {
        plpgsql_inline_handler(codeblock)
    });

    // Install the PL/pgSQL expression evaluator's SPI bridge (the executor's
    // `exec_eval_expr` → `exec_run_select` slow path). The executor unit is
    // layered below SPI and reaches the SPI plan surface through this seam; the
    // handler (the top layer, with SPI access) installs it. The exec-seams
    // bridge types map 1:1 to the SPI `spi_eval_expr` value types.
    backend_pl_plpgsql_exec_seams::exec_eval_expr_via_spi::set(
        |query: String,
         parse_mode,
         parse_state,
         datum_snapshot: Vec<Option<backend_pl_plpgsql_exec_seams::EvalParamValue>>,
         maxtuples,
         read_only| {
            let mut resolve = |dno: i32| -> PgResult<backend_executor_spi::EvalParamValue> {
                // setup_param_list reads estate->datums[dno]; the snapshot carries
                // the value the caller (exec) read out of the live execstate.
                match datum_snapshot.get(dno as usize).and_then(|o| o.as_ref()) {
                    Some(v) => Ok(backend_executor_spi::EvalParamValue {
                        value: v.value,
                        isnull: v.isnull,
                        typeid: v.typeid,
                        // A by-reference datum carries its image; forward it so
                        // the param-bind reconstructs the rich `Datum::ByRef`.
                        byref: v.byref.clone(),
                    }),
                    None => Err(types_error::PgError::error(format!(
                        "PL/pgSQL expression references datum {dno} that is not a scalar variable"
                    ))),
                }
            };
            let r = backend_executor_spi::spi_eval_expr(
                &query,
                parse_mode,
                parse_state,
                maxtuples,
                read_only,
                &mut resolve,
            )?;
            Ok(backend_pl_plpgsql_exec_seams::EvalExprResult {
                value: r.value,
                isnull: r.isnull,
                byref: r.byref,
                typeid: r.typeid,
                processed: r.processed,
            })
        },
    );

    // Install the final RAISE `ereport(stmt->elog_level, ...)` (pl_exec.c): drive
    // the elog report cycle with the assembled fields. A non-ERROR level reports
    // a message to the client and returns Ok; an ERROR throws (Err).
    backend_pl_plpgsql_exec_seams::raise_ereport::set(
        |report: backend_pl_plpgsql_exec_seams::RaiseEreport| {
            use backend_utils_error::ereport;
            use types_error::{ErrorLocation, ErrorLevel, SqlState};

            let mut b = ereport(ErrorLevel(report.elog_level));
            if report.err_code != 0 {
                b = b.errcode(SqlState(report.err_code));
            }
            // errmsg_internal("%s", err_message) — the message is already the
            // final text (no further %-substitution).
            b = b.errmsg_internal(report.message);
            if let Some(d) = report.detail {
                b = b.errdetail_internal(d);
            }
            if let Some(h) = report.hint {
                b = b.errhint_internal(h);
            }
            // err_generic_string(PG_DIAG_*, value) for the diagnostics fields.
            if let Some(v) = report.column {
                b = b.err_generic_string(types_error::PG_DIAG_COLUMN_NAME, v)?;
            }
            if let Some(v) = report.constraint {
                b = b.err_generic_string(types_error::PG_DIAG_CONSTRAINT_NAME, v)?;
            }
            if let Some(v) = report.datatype {
                b = b.err_generic_string(types_error::PG_DIAG_DATATYPE_NAME, v)?;
            }
            if let Some(v) = report.table {
                b = b.err_generic_string(types_error::PG_DIAG_TABLE_NAME, v)?;
            }
            if let Some(v) = report.schema {
                b = b.err_generic_string(types_error::PG_DIAG_SCHEMA_NAME, v)?;
            }
            // errcontext(): the PL/pgSQL error-context line for a non-ERROR
            // report, supplied by the executor (the ERROR path attaches its own
            // context on propagation). C's error_context_stack callbacks add
            // this at report time for every elevel.
            if let Some(ctx) = report.context {
                if !ctx.is_empty() {
                    b = b.errcontext_msg(ctx);
                }
            }
            b.finish(ErrorLocation {
                filename: None,
                lineno: 0,
                funcname: None,
            })
        },
    );

    // Install `convert_value_to_string` (pl_exec.c): getTypeOutputInfo +
    // OidOutputFunctionCall. The executor uses it for RAISE `%` substitution and
    // USING option text. The plpgsql value crosses as a bare word; for a
    // by-value type that is the value itself (a by-ref result is the separate
    // by-ref-Datum keystone, which the output function would dereference).
    backend_pl_plpgsql_exec_seams::convert_value_to_string::set(
        |value: usize, byref: Option<Vec<u8>>, valtype| {
            let cxt = mcx::MemoryContext::new("PL/pgSQL convert_value_to_string");
            let mcx = cxt.mcx();
            let (typoutput, typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(valtype)?;
            // For a pass-by-reference type the bare word is `0` and the referent
            // varlena/cstring image is carried out-of-band in `byref`; build the
            // canonical by-ref Datum so the output function reads the real value
            // (C: the Datum *is* the pointer). A by-value type uses the bare word.
            use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
            let datum = match byref {
                Some(image) if typisvarlena => CanonDatum::ByRef(mcx::slice_in(mcx, &image)?),
                Some(image) => {
                    // A pass-by-reference but non-varlena type. Two sub-cases,
                    // distinguished by typlen (C: the Datum is just the pointer,
                    // and the output function knows its own typlen):
                    //   typlen == -2  => `cstring`: the image is a NUL-terminated
                    //                    C string; cross the `Cstring` lane.
                    //   typlen  >  0  => fixed-length by-ref (e.g. `name`,
                    //                    NAMEDATALEN=64): the image is the raw
                    //                    NUL-padded buffer, which crosses the
                    //                    by-ref lane VERBATIM as `ByRef` (the
                    //                    raw-buffer convention `arg_name` reads via
                    //                    `as_varlena`). Routing it through the
                    //                    `Cstring` lane is the
                    //                    "name arg missing from by-ref lane" bug.
                    let typlen = backend_utils_cache_lsyscache_seams::get_typlen::call(valtype)?;
                    if typlen == -2 {
                        match std::str::from_utf8(&image) {
                            Ok(s) => CanonDatum::Cstring(s.to_string()),
                            Err(_) => CanonDatum::ByRef(mcx::slice_in(mcx, &image)?),
                        }
                    } else {
                        CanonDatum::ByRef(mcx::slice_in(mcx, &image)?)
                    }
                }
                None => CanonDatum::from_usize(value),
            };
            let bytes = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(
                mcx, typoutput, &datum,
            )?;
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        },
    );

    // Install `plpgsql_recognize_err_condition` (pl_comp.c) — bridge to the
    // compiler's real body (the exception-label table + SQLSTATE parse).
    backend_pl_plpgsql_exec_seams::recognize_err_condition::set(
        |condname: String, allow_sqlstate| {
            backend_pl_plpgsql_comp::plpgsql_recognize_err_condition(&condname, allow_sqlstate)
        },
    );

    // Install `exec_cast_value` slow path (pl_exec.c do_cast_value): the
    // CaseTestExpr coercion-expression + ExecEvalExpr machinery is executor
    // substrate; plpgsql's documented fallback for any coercion not available as
    // a plan-level cast is an I/O coercion (CoerceViaIO), which is faithful and
    // correct for scalar casts. We implement the slow path as that I/O coercion:
    // render the source value to text (source typoutput) and read it back at the
    // target type (target typinput, with the target typmod). The no-op relabel
    // case (valtype == reqtype, unconstrained typmod) is handled in-crate and
    // never reaches here.
    backend_pl_plpgsql_exec_seams::exec_cast_value_via_spi::set(
        |value: usize, value_byref, isnull, valtype, _valtypmod, reqtype, reqtypmod| {
            use backend_pl_plpgsql_exec_seams::CastValueResult;
            if isnull {
                // A NULL normally stays NULL across an I/O coercion (the cast is
                // strict). But when the target is a DOMAIN, C's do_cast_value
                // still runs the cast expression (which contains a CoerceToDomain
                // node) so the domain's NOT NULL / CHECK constraints get a chance
                // to reject the NULL. Mirror that by running the target type's
                // input function on a NULL cstring: a domain's typinput is
                // domain_in, which enforces the constraints; a base type's input
                // is strict and returns NULL unchanged.
                const TYPTYPE_DOMAIN: u8 = b'd';
                if backend_utils_cache_lsyscache_seams::get_typtype::call(reqtype)?
                    == TYPTYPE_DOMAIN
                {
                    let cxt = mcx::MemoryContext::new("PL/pgSQL exec_cast_value (null domain)");
                    let mcx = cxt.mcx();
                    let (typinput, typioparam) =
                        backend_utils_cache_lsyscache_seams::get_type_input_info::call(reqtype)?;
                    // domain_in(NULL) returns NULL when the domain permits it; if a
                    // NOT NULL / CHECK rejects it, the call below already raises.
                    let _ = backend_utils_fmgr_fmgr_seams::input_function_call::call(
                        mcx, typinput, None, typioparam, reqtypmod, None,
                    )?;
                    return Ok(CastValueResult { value: 0, isnull: true, byref: None });
                }
                return Ok(CastValueResult { value, isnull: true, byref: None });
            }
            let cxt = mcx::MemoryContext::new("PL/pgSQL exec_cast_value");
            let mcx = cxt.mcx();

            // Rebuild the canonical source `Datum`: a by-reference source carries
            // its image in `value_byref` (the bare `value` word is `0` then); a
            // by-value source is the bare scalar word.
            use types_tuple::backend_access_common_heaptuple::Datum as CastDatum;
            let src_datum = match &value_byref {
                Some(image) => CastDatum::ByRef(mcx::slice_in(mcx, image)?),
                None => CastDatum::from_usize(value),
            };

            // get_cast_hashentry / do_cast_value (pl_exec.c): resolve the real
            // coercion pathway under COERCION_PLPGSQL. A function-based cast
            // (COERCION_PATH_FUNC) must run its cast function — e.g. a
            // SQL-function cast like `sql_to_date(int)` — rather than be bypassed
            // by a plain I/O coercion. The C path builds a FuncExpr over the
            // value and ExecEvalExpr's it; the owned model invokes the cast
            // function directly through fmgr (which enters fmgr_sql for SQL
            // casts, producing the `SQL function "..."` error context). Only the
            // function and binary-coercible (relabel) cases are handled here;
            // everything else (no path / I/O coercion / array coercion) falls
            // through to the historical I/O coercion below.
            {
                let (pathtype, funcid) =
                    backend_parser_coerce_seams::find_coercion_pathway_plpgsql::call(
                        reqtype, valtype,
                    )?;
                match pathtype {
                    backend_parser_coerce_seams::CoercionPathType::Func => {
                        // The cast function takes (value), (value, typmod), or
                        // (value, typmod, isExplicit). COERCION_PLPGSQL builds an
                        // implicit-format cast, so isExplicit is false.
                        let finfo =
                            backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, funcid)?;
                        let result = if finfo.fn_nargs >= 3 {
                            backend_utils_fmgr_fmgr_seams::function_call3_coll_datum::call(
                                mcx,
                                funcid,
                                src_datum.clone(),
                                CastDatum::from_i32(reqtypmod),
                                CastDatum::from_i32(0), // isExplicit = false
                            )?
                        } else if finfo.fn_nargs == 2 {
                            backend_utils_fmgr_fmgr_seams::function_call2_coll_datum::call(
                                mcx,
                                funcid,
                                types_core::INVALID_OID,
                                src_datum.clone(),
                                CastDatum::from_i32(reqtypmod),
                            )?
                        } else {
                            backend_utils_fmgr_fmgr_seams::function_call1_coll_datum::call(
                                mcx,
                                funcid,
                                types_core::INVALID_OID,
                                src_datum.clone(),
                            )?
                        };
                        let out = match result {
                            CastDatum::ByVal(w) => {
                                CastValueResult { value: w, isnull: false, byref: None }
                            }
                            CastDatum::ByRef(b) => CastValueResult {
                                value: 0,
                                isnull: false,
                                byref: Some(b.as_slice().to_vec()),
                            },
                            CastDatum::Cstring(ref sct) => CastValueResult {
                                value: 0,
                                isnull: false,
                                byref: Some(sct.as_bytes().to_vec()),
                            },
                            other => CastValueResult {
                                value: 0,
                                isnull: false,
                                byref: Some(other.as_varlena_bytes().into_owned()),
                            },
                        };
                        return Ok(out);
                    }
                    backend_parser_coerce_seams::CoercionPathType::Relabeltype => {
                        // Binary-coercible: no function, the value passes through
                        // unchanged (the RelabelType no-op in get_cast_hashentry).
                        return Ok(CastValueResult { value, isnull: false, byref: value_byref });
                    }
                    // None / Coerceviaio / Arraycoerce: fall through to the
                    // historical I/O coercion below (faithful for I/O casts, and
                    // plpgsql's documented fallback when no assignment cast path
                    // exists).
                    _ => {}
                }
            }

            // Render the source value to its text representation, then read it
            // back at the target type (the I/O coercion fallback).
            let (typoutput, _typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(valtype)?;
            let text = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(
                mcx, typoutput, &src_datum,
            )?;
            let s = String::from_utf8_lossy(&text).into_owned();

            // Read it back at the target type with the target typmod.
            let (typinput, typioparam) =
                backend_utils_cache_lsyscache_seams::get_type_input_info::call(reqtype)?;
            let result = backend_utils_fmgr_fmgr_seams::input_function_call::call(
                mcx,
                typinput,
                Some(s.as_str()),
                typioparam,
                reqtypmod,
                None,
            )?;
            // The coerced result crosses back either as a by-value bare word or —
            // for a pass-by-reference target (`text`/`varchar`/`numeric`/…) — as
            // its owned varlena / cstring image, `datumCopy`'d out of `mcx` so it
            // outlives this working context (mirrors the SPI receiver capture).
            let out = match result {
                types_tuple::backend_access_common_heaptuple::Datum::ByVal(w) => {
                    CastValueResult { value: w, isnull: false, byref: None }
                }
                types_tuple::backend_access_common_heaptuple::Datum::ByRef(b) => {
                    CastValueResult { value: 0, isnull: false, byref: Some(b.as_slice().to_vec()) }
                }
                types_tuple::backend_access_common_heaptuple::Datum::Cstring(ref sct) => {
                    CastValueResult {
                        value: 0,
                        isnull: false,
                        byref: Some(sct.as_bytes().to_vec()),
                    }
                }
                other => CastValueResult {
                    value: 0,
                    isnull: false,
                    byref: Some(other.as_varlena_bytes().into_owned()),
                },
            };
            Ok(out)
        },
    );

    // Install `exec_stmt_execsql` core (pl_exec.c): the SPI plan surface for an
    // embedded DML / SELECT statement (the bridge types map 1:1 to the SPI
    // spi_execsql value types, like exec_eval_expr_via_spi).
    backend_pl_plpgsql_exec_seams::exec_execsql_via_spi::set(
        |query: String,
         parse_mode,
         parse_state,
         datum_snapshot: Vec<Option<backend_pl_plpgsql_exec_seams::EvalParamValue>>,
         read_only,
         into,
         tcount| {
            let mut resolve = |dno: i32| -> PgResult<backend_executor_spi::EvalParamValue> {
                match datum_snapshot.get(dno as usize).and_then(|o| o.as_ref()) {
                    Some(v) => Ok(backend_executor_spi::EvalParamValue {
                        value: v.value,
                        isnull: v.isnull,
                        typeid: v.typeid,
                        // A by-reference datum carries its image; forward it so
                        // the param-bind reconstructs the rich `Datum::ByRef`.
                        byref: v.byref.clone(),
                    }),
                    None => Err(types_error::PgError::error(format!(
                        "PL/pgSQL embedded SQL references datum {dno} that is not a scalar variable"
                    ))),
                }
            };
            let r = backend_executor_spi::spi_execsql(
                &query,
                parse_mode,
                parse_state,
                read_only,
                into,
                tcount,
                &mut resolve,
            )?;
            Ok(backend_pl_plpgsql_exec_seams::ExecsqlResult {
                code: r.code,
                processed: r.processed,
                returned_tuptable: r.returned_tuptable,
                first_row: r
                    .first_row
                    .into_iter()
                    .map(|c| backend_pl_plpgsql_exec_seams::ExecsqlColumn {
                        value: c.value,
                        isnull: c.isnull,
                        typeid: c.typeid,
                        typmod: -1,
                        name: c.name,
                        // A by-reference INTO column carries its image; forward
                        // it so the INTO store keeps the image in the target var.
                        byref: c.byref,
                    })
                    .collect(),
            })
        },
    );

    // Install `exec_run_select` materialize-all (pl_exec.c): the FOR-loop /
    // RETURN QUERY iteration path. Runs the query through the SPI plan surface
    // and hands back every result row's columns (the materialize-all analogue of
    // C's portal-fetch loop; SPI_cursor_open is a separate keystone, so this
    // collects all rows up front — the observable iteration is identical).
    backend_pl_plpgsql_exec_seams::exec_run_select_via_spi::set(
        |query: String,
         parse_mode,
         parse_state,
         datum_snapshot: Vec<Option<backend_pl_plpgsql_exec_seams::EvalParamValue>>,
         read_only,
         must_return_tuples| {
            let mut resolve = |dno: i32| -> PgResult<backend_executor_spi::EvalParamValue> {
                match datum_snapshot.get(dno as usize).and_then(|o| o.as_ref()) {
                    Some(v) => Ok(backend_executor_spi::EvalParamValue {
                        value: v.value,
                        isnull: v.isnull,
                        typeid: v.typeid,
                        byref: v.byref.clone(),
                    }),
                    None => Err(types_error::PgError::error(format!(
                        "PL/pgSQL FOR-query references datum {dno} that is not a scalar variable"
                    ))),
                }
            };
            let r = backend_executor_spi::spi_execsql_collect(
                &query,
                parse_mode,
                parse_state,
                read_only,
                must_return_tuples,
                &mut resolve,
            )?;
            Ok(backend_pl_plpgsql_exec_seams::RunSelectResult {
                code: r.code,
                processed: r.processed,
                returned_tuptable: r.returned_tuptable,
                all_rows: r
                    .all_rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|c| backend_pl_plpgsql_exec_seams::ExecsqlColumn {
                                value: c.value,
                                isnull: c.isnull,
                                typeid: c.typeid,
                                typmod: -1,
                                name: c.name,
                                byref: c.byref,
                            })
                            .collect()
                    })
                    .collect(),
            })
        },
    );

    // Install `exec_stmt_dynexecute` / `exec_dynquery_with_params` core
    // (pl_exec.c): the SPI one-shot surface for a dynamic EXECUTE query string,
    // with already-evaluated USING params. Runs SELECT / DML / utility; INTO
    // collects the first row, FOR-IN-EXECUTE collects every row.
    backend_pl_plpgsql_exec_seams::exec_dynexecute_via_spi::set(
        |query: String,
         params: Vec<backend_pl_plpgsql_exec_seams::DynUsingParam>,
         read_only,
         into,
         collect_all,
         tcount,
         must_return_tuples| {
            let using: Vec<backend_executor_spi::EvalParamValue> = params
                .into_iter()
                .map(|p| backend_executor_spi::EvalParamValue {
                    value: p.value,
                    isnull: p.isnull,
                    typeid: p.typeid,
                    byref: p.byref,
                })
                .collect();
            let r = backend_executor_spi::spi_execsql_dynamic(
                &query, &using, read_only, into, collect_all, tcount, must_return_tuples,
            )?;
            let map_col = |c: backend_executor_spi::ExecsqlColumn| {
                backend_pl_plpgsql_exec_seams::ExecsqlColumn {
                    value: c.value,
                    isnull: c.isnull,
                    typeid: c.typeid,
                    typmod: -1,
                    name: c.name,
                    byref: c.byref,
                }
            };
            Ok(backend_pl_plpgsql_exec_seams::DynExecResult {
                code: r.code,
                processed: r.processed,
                returned_tuptable: r.returned_tuptable,
                first_row: r.first_row.into_iter().map(map_col).collect(),
                all_rows: r
                    .all_rows
                    .into_iter()
                    .map(|row| row.into_iter().map(map_col).collect())
                    .collect(),
            })
        },
    );

    // Install the PL/pgSQL cursor surface (pl_exec.c's exec_stmt_open /
    // exec_stmt_fetch / exec_stmt_close / exec_stmt_forc) over the SPI cursor
    // functions. The executor unit is layered below the SPI cursor/portal
    // surface, so it reaches it through these seams; the handler (top layer
    // with SPI access) installs them — thin marshal + delegate, no behavior.

    // `SPI_cursor_open_with_paramlist` over a static OPEN query.
    backend_pl_plpgsql_exec_seams::spi_cursor_open::set(
        |curname: Option<String>,
         query: String,
         parse_mode,
         parse_state,
         cursor_options,
         read_only,
         datum_snapshot: Vec<Option<backend_pl_plpgsql_exec_seams::EvalParamValue>>| {
            let mut resolve = |dno: i32| -> PgResult<backend_executor_spi::EvalParamValue> {
                match datum_snapshot.get(dno as usize).and_then(|o| o.as_ref()) {
                    Some(v) => Ok(backend_executor_spi::EvalParamValue {
                        value: v.value,
                        isnull: v.isnull,
                        typeid: v.typeid,
                        byref: v.byref.clone(),
                    }),
                    None => Err(types_error::PgError::error(format!(
                        "PL/pgSQL cursor query references datum {dno} that is not a scalar variable"
                    ))),
                }
            };
            backend_executor_spi::spi_cursor_open_plpgsql(
                curname.as_deref(),
                &query,
                parse_mode,
                parse_state,
                cursor_options,
                read_only,
                &mut resolve,
            )
        },
    );

    // `SPI_cursor_parse_open` over an OPEN ... FOR EXECUTE dynamic query string.
    backend_pl_plpgsql_exec_seams::spi_cursor_open_execute::set(
        |curname: Option<String>,
         query: String,
         params: Vec<backend_pl_plpgsql_exec_seams::DynUsingParam>,
         cursor_options,
         read_only| {
            let using: Vec<backend_executor_spi::EvalParamValue> = params
                .into_iter()
                .map(|p| backend_executor_spi::EvalParamValue {
                    value: p.value,
                    isnull: p.isnull,
                    typeid: p.typeid,
                    byref: p.byref,
                })
                .collect();
            backend_executor_spi::spi_cursor_parse_open(
                curname.as_deref(),
                &query,
                &using,
                cursor_options,
                read_only,
            )
        },
    );

    // `SPI_cursor_find(name)` — does a cursor of this name exist?
    backend_pl_plpgsql_exec_seams::spi_cursor_find::set(|name: String| {
        backend_executor_spi::spi_cursor_find(&name)
    });

    // `SPI_scroll_cursor_fetch` / `SPI_scroll_cursor_move` (FETCH / MOVE).
    backend_pl_plpgsql_exec_seams::spi_cursor_fetch_move::set(
        |name: String,
         direction: backend_pl_plpgsql_exec_seams::CursorFetchDirection,
         count: i64,
         is_move: bool| {
            use backend_pl_plpgsql_exec_seams::CursorFetchDirection as CFD;
            use types_portal::FetchDirection as PFD;
            let dir = match direction {
                CFD::Forward => PFD::FETCH_FORWARD,
                CFD::Backward => PFD::FETCH_BACKWARD,
                CFD::Absolute => PFD::FETCH_ABSOLUTE,
                CFD::Relative => PFD::FETCH_RELATIVE,
            };
            let r = backend_executor_spi::spi_cursor_fetch_move(&name, dir, count, is_move)?;
            Ok(backend_pl_plpgsql_exec_seams::CursorFetchResult {
                processed: r.processed,
                rows: r
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|c| backend_pl_plpgsql_exec_seams::ExecsqlColumn {
                                value: c.value,
                                isnull: c.isnull,
                                typeid: c.typeid,
                                typmod: -1,
                                name: c.name,
                                byref: c.byref,
                            })
                            .collect()
                    })
                    .collect(),
            })
        },
    );

    // `SPI_cursor_close(portal)` (CLOSE).
    backend_pl_plpgsql_exec_seams::spi_cursor_close::set(|name: String| {
        backend_executor_spi::spi_cursor_close_by_name(&name)
    });

    // Install the array-iteration leg of `exec_stmt_foreach_a` (pl_exec.c). The
    // executor unit is layered below the array/lsyscache owners; the handler
    // (which depends on backend-utils-adt-arrayfuncs + lsyscache) drives the C
    // steps `get_element_type` / `DatumGetArrayTypePCopy` / the slice range
    // check / `array_create_iterator` + the full `array_iterate` loop, and
    // materializes every element/slice as a `ForeachItem` in iteration order.
    backend_pl_plpgsql_exec_seams::foreach_iterate_via_array::set(
        foreach_iterate_via_array_impl,
    );
    // `get_element_type` for exec_stmt_foreach_a's loop-variable array-ness check.
    backend_pl_plpgsql_exec_seams::foreach_get_element_type::set(|typid: types_core::Oid| {
        backend_utils_cache_lsyscache_seams::get_element_type::call(typid)
    });

    // `plpgsql_check_asserts` GUC read for exec_stmt_assert. The GUC variable is
    // owned in this unit (pl_handler.c); the executor reads it through the seam.
    backend_pl_plpgsql_exec_seams::plpgsql_check_asserts::set(plpgsql_check_asserts);

    // `plpgsql_extra_warnings` / `plpgsql_extra_errors` live-GUC reads for the
    // executor's runtime too-many-rows / strict-multi-assignment checks.
    backend_pl_plpgsql_exec_seams::plpgsql_extra_warnings::set(plpgsql_extra_warnings);
    backend_pl_plpgsql_exec_seams::plpgsql_extra_errors::set(plpgsql_extra_errors);

    // `type_is_rowtype` for exec_stmt_return's composite-result test.
    backend_pl_plpgsql_exec_seams::type_is_rowtype::set(|typid: types_core::Oid| {
        backend_utils_cache_lsyscache_seams::type_is_rowtype::call(typid)
    });

    // `exec_eval_datum` DTYPE_ROW (pl_exec.c 5316): BlessTupleDesc the compiled
    // row's rowtupdesc, then `make_tuple_from_row` (heap_form_tuple over the
    // executor-evaluated field values) and `HeapTupleGetDatum`. The executor
    // reads each field and hands them here; the handler — the top layer above
    // execTuples (BlessTupleDesc), heaptuple (heap_form_tuple /
    // HeapTupleGetDatum) and the compiler's backend-lifetime rowtupdesc table —
    // forms the composite Datum image.
    backend_pl_plpgsql_exec_seams::form_row_composite_datum::set(
        |fields: Vec<backend_pl_plpgsql_exec_seams::ExecsqlColumn>, rowtupdesc_handle: u64| {
            use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;

            let cxt = mcx::MemoryContext::new("PL/pgSQL make_tuple_from_row");
            let mcx = cxt.mcx();

            // BlessTupleDesc(row->rowtupdesc): for an anonymous RECORD descriptor
            // (the OUT-parameter row), register a transient record type and stamp
            // (tdtypeid=RECORDOID, tdtypmod=<assigned>) back into the live
            // backend-lifetime descriptor. Then read its reported (typeid,
            // typmod) and natts/attr typeids to mirror make_tuple_from_row.
            //
            // The descriptor lives in the compiler's rowtupdesc_table; borrow it
            // mutably so the bless persists on the cached descriptor (as C blesses
            // the long-lived row->rowtupdesc).
            let formed = backend_pl_plpgsql_comp::rowtupdesc_table::with_rowtupdesc(
                rowtupdesc_handle,
                |td: &mut types_tuple::heaptuple::TupleDescData<'static>|
                 -> PgResult<backend_pl_plpgsql_exec_seams::RowCompositeDatum> {
                    // BlessTupleDesc's guard: only an anonymous RECORD descriptor
                    // is registered; a named-composite descriptor keeps its id.
                    // RECORDOID (2249) — an anonymous composite descriptor.
                    if td.tdtypeid == 2249 && td.tdtypmod < 0 {
                        backend_utils_cache_typcache_seams::assign_record_type_typmod::call(td)?;
                    }
                    let natts = td.natts as usize;
                    // make_tuple_from_row: natts != row->nfields → NULL.
                    if natts != fields.len() {
                        return Err(types_error::PgError::error(
                            "row not compatible with its own tupdesc",
                        ));
                    }
                    let mut values: Vec<CanonDatum> = Vec::with_capacity(natts);
                    let mut nulls: Vec<bool> = Vec::with_capacity(natts);
                    for (i, f) in fields.iter().enumerate() {
                        let att = &td.attrs[i];
                        if att.attisdropped {
                            // Dropped column → leave it NULL (the field's varno
                            // was negative, so the executor passed isnull=true).
                            values.push(CanonDatum::null());
                            nulls.push(true);
                            continue;
                        }
                        // fieldtypeid != atttypid → make_tuple_from_row NULL.
                        if f.typeid != att.atttypid {
                            return Err(types_error::PgError::error(
                                "row not compatible with its own tupdesc",
                            ));
                        }
                        if f.isnull {
                            values.push(CanonDatum::null());
                            nulls.push(true);
                        } else if let Some(bytes) = &f.byref {
                            values.push(CanonDatum::from_byref_bytes_in(mcx, bytes)?);
                            nulls.push(false);
                        } else {
                            values.push(CanonDatum::from_usize(f.value));
                            nulls.push(false);
                        }
                    }
                    // heap_form_tuple(tupdesc, dvalues, nulls).
                    let tuple = backend_access_common_heaptuple::heap_form_tuple(
                        mcx, td, &values, &nulls,
                    )
                    .map_err(|e| {
                        types_error::PgError::error(format!("heap_form_tuple failed: {e:?}"))
                    })?;
                    // HeapTupleGetDatum: set the composite-Datum header fields
                    // (datum length / typeid / typmod) and serialize the flat
                    // varlena image C points a composite Datum at.
                    let composite =
                        backend_access_common_heaptuple::heap_copy_tuple_as_datum(
                            mcx, &tuple, td,
                        )?;
                    Ok(backend_pl_plpgsql_exec_seams::RowCompositeDatum {
                        image: composite.to_datum_image(),
                        typeid: td.tdtypeid,
                        typmod: td.tdtypmod,
                    })
                },
            );
            match formed {
                Some(res) => res,
                // handle == 0 / out-of-range: C's "row variable has no tupdesc"
                // (the executor already guards handle == 0 before calling, so an
                // out-of-range handle is the only path here).
                None => Err(types_error::PgError::error("row variable has no tupdesc")),
            }
        },
    );

    // Install the `exec_stmt_block` EXCEPTION-leg subtransaction entry points
    // (pl_exec.c keystone #215). The executor unit is layered below xact; the
    // handler (top layer) bridges to the now-ported xact subxact engine. These
    // are thin delegations — no behavior is added.
    backend_pl_plpgsql_exec_seams::begin_internal_subtransaction::set(|| {
        // BeginInternalSubTransaction(NULL).
        backend_access_transam_xact::BeginInternalSubTransaction(None)
    });
    backend_pl_plpgsql_exec_seams::release_current_subtransaction::set(|| {
        backend_access_transam_xact::ReleaseCurrentSubTransaction()
    });
    backend_pl_plpgsql_exec_seams::rollback_and_release_current_subtransaction::set(|| {
        // xact's AbortSubTransaction drives AtEOSubXact_SPI(false, mySubid)
        // through the installed seam, restoring the SPI connection (modern PG
        // dropped the explicit SPI_restore_connection call here).
        backend_access_transam_xact::RollbackAndReleaseCurrentSubTransaction()
    });

    // `oldowner = CurrentResourceOwner` / `CurrentResourceOwner = oldowner` —
    // the snapshot+restore pl_exec.c's exec_stmt_block performs around the
    // internal subtransaction. The subxact engine's CleanupSubTransaction leaves
    // CurrentResourceOwner at the parent (CurTransaction) owner; without this
    // restore, the outer statement's relation refs / buffer pins (opened under
    // the portal's resource owner) are later forgotten under the wrong owner.
    // `ResourceOwner::NULL` (the C NULL owner) round-trips faithfully.
    backend_pl_plpgsql_exec_seams::current_resource_owner::set(|| {
        backend_utils_resowner_resowner_seams::CurrentResourceOwner::call()
    });
    backend_pl_plpgsql_exec_seams::set_current_resource_owner::set(|owner| {
        backend_utils_resowner_resowner_seams::set_CurrentResourceOwner::call(owner)
    });

    // Install `CStringGetTextDatum` for the EXCEPTION handler's SQLSTATE/SQLERRM
    // special-var binding (assign_error_vars) and `exec_stmt_getdiag`. C does
    // `CStringGetTextDatum(s)` = `cstring_to_text(s)` palloc'd in
    // `CurrentMemoryContext`, then `exec_assign_value(..., TEXTOID, -1)` stores
    // it into the target variable (assign_simple_var copies it into the
    // function's datum context with datumCopy). The PL/pgSQL executor here
    // carries scalar values as bare machine words (`types_datum::Datum`), so a
    // by-reference `text` must cross as a pointer word at a header-ful varlena
    // whose bytes outlive the call. Build that header-ful varlena in a
    // backend-lifetime context (mirroring the palloc) and return the pointer
    // word; the target var keeps it (these short error strings are bounded).
    backend_pl_plpgsql_exec_seams::cstring_to_text_datum::set(|s: String| {
        // A leaked, backend-lifetime context: the produced `text` outlives this
        // call (it is stored into a PL/pgSQL variable), exactly as C's
        // CStringGetTextDatum palloc lives in the function's execution context.
        let ctx: &'static mcx::MemoryContext = PLPGSQL_ERRVAR_CONTEXT.with(|c| {
            *c.get_or_init(|| {
                Box::leak(Box::new(mcx::MemoryContext::new("PL/pgSQL error-var text")))
            })
        });
        let bytes = s.as_bytes();
        // C: palloc(len + VARHDRSZ); SET_VARSIZE; memcpy(VARDATA, s, len).
        let mut image = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), bytes.len() + VARHDRSZ)?;
        image.extend_from_slice(&[0u8; VARHDRSZ]);
        image.extend_from_slice(bytes);
        let varlena = types_datum::Varlena::from_image(image);
        // DatumGetPointer view: the address of the header-ful varlena image. The
        // image lives in the leaked backend-lifetime context; forget the owning
        // wrapper so its Drop never deallocates (the bytes must persist for the
        // lifetime of the variable that now holds the pointer, like C's palloc).
        let image = varlena.into_image();
        let ptr = image.as_ptr() as usize;
        // Hand back a copy of the verbatim header-ful varlena bytes so the caller
        // (`assign_text_var`) can populate the variable's `value_byref`
        // out-of-band companion. Without this image the special var would carry
        // only the bare word, and a later read across the fmgr boundary (e.g.
        // `RETURN SQLERRM`, a text comparison) would see no by-ref payload and
        // panic in the varlena cmp cores.
        let image_copy = image.to_vec();
        core::mem::forget(image);
        Ok((ptr, image_copy))
    });

    // Install `namein`-style `name` Datum construction for the `name`-typed
    // trigger promises (TG_NAME / TG_TABLE_NAME / TG_TABLE_SCHEMA). A `name`
    // value crosses the PL/pgSQL scalar boundary as a bare-word pointer at a
    // fixed 64-byte NUL-padded `NameData` buffer (the raw-name convention), in a
    // backend-lifetime context like the SQLERRM text image above.
    backend_pl_plpgsql_exec_seams::cstring_to_name_datum::set(|s: String| {
        const NAMEDATALEN: usize = 64;
        let bytes = s.as_bytes();
        // namein truncates at NAMEDATALEN-1 and NUL-pads to NAMEDATALEN.
        let n = bytes.len().min(NAMEDATALEN - 1);
        let mut image = vec![0u8; NAMEDATALEN];
        image[..n].copy_from_slice(&bytes[..n]);
        Ok(image)
    });

    // `get_namespace_name(nspoid)` for TG_TABLE_SCHEMA — delegate to lsyscache.
    backend_pl_plpgsql_exec_seams::get_namespace_name::set(|nspoid| {
        let ctx = mcx::MemoryContext::new("PL/pgSQL nspname scratch");
        let r = backend_utils_cache_lsyscache_seams::get_namespace_name::call(ctx.mcx(), nspoid)?
            .map(|s| s.as_str().to_string());
        match r {
            Some(s) => Ok(s),
            None => Err(types_error::PgError::error(format!(
                "cache lookup failed for namespace {nspoid}"
            ))),
        }
    });

    // `construct_array(elems, n, TEXTOID, -1, false, 'i')` for the TG_ARGV
    // text[] promise. Each element is a header-ful `text` varlena; the resulting
    // array varlena rides a bare-word pointer in a backend-lifetime context.
    backend_pl_plpgsql_exec_seams::construct_text_array_datum::set(|elems| {
        const TEXTOID: types_core::Oid = 25;
        const TYPALIGN_INT: u8 = b'i';
        let ctx: &'static mcx::MemoryContext = PLPGSQL_ERRVAR_CONTEXT.with(|c| {
            *c.get_or_init(|| {
                Box::leak(Box::new(mcx::MemoryContext::new("PL/pgSQL error-var text")))
            })
        });
        let mcx = ctx.mcx();
        // Build a bare-word `text` element Datum (a pointer at a header-ful
        // varlena image in `mcx`) per argument; a NULL element rides the nulls
        // bitmap. The images live in the leaked context, so the pointers stay
        // valid through construct_md_array (which copies the bytes in).
        let mut datums: Vec<types_datum::datum::Datum> = Vec::with_capacity(elems.len());
        let mut nulls: Vec<bool> = Vec::with_capacity(elems.len());
        for e in &elems {
            match e {
                Some(bytes) => {
                    let mut image = mcx::vec_with_capacity_in::<u8>(mcx, bytes.len() + VARHDRSZ)?;
                    image.extend_from_slice(&[0u8; VARHDRSZ]);
                    image.extend_from_slice(bytes);
                    let image = types_datum::Varlena::from_image(image).into_image();
                    datums.push(types_datum::datum::Datum::from_usize(image.as_ptr() as usize));
                    core::mem::forget(image);
                    nulls.push(false);
                }
                None => {
                    datums.push(types_datum::datum::Datum::from_usize(0));
                    nulls.push(true);
                }
            }
        }
        let has_nulls = nulls.iter().any(|&n| n);
        let dims = [datums.len() as i32];
        // For historical reasons TG_ARGV[] subscripts start at zero, not one
        // (pl_exec.c PLPGSQL_PROMISE_TG_ARGV: lbs[0] = 0); that is exactly why
        // C builds it with construct_md_array rather than construct_array.
        let lbs = [0i32];
        let arr = backend_utils_adt_arrayfuncs::construct::construct_md_array(
            mcx,
            &datums,
            if has_nulls { Some(&nulls) } else { None },
            1,
            &dims,
            &lbs,
            TEXTOID,
            -1,
            false,
            TYPALIGN_INT,
        )?;
        // Hand back the verbatim array varlena byte image (the by-ref lane
        // carries it into the TG_ARGV variable).
        Ok(arr.as_slice().to_vec())
    });

    register_handler_builtins();

    // Register `$libdir/plpgsql` with the in-process ported-library loader
    // registry, so fmgr's LANGUAGE C resolution of the call handler / inline
    // handler / validator (pg_proc.probin = '$libdir/plpgsql') finds the ported
    // PGFunctions instead of trying to dlopen a nonexistent plpgsql.so.
    backend_utils_fmgr_dfmgr_seams::register_builtin_library(
        backend_utils_fmgr_dfmgr_seams::BuiltinLibraryEntry {
            name: LIBRARY,
            lookup,
            // `LOAD 'plpgsql'` runs plpgsql's `_PG_init` slice: the custom-GUC
            // registration + `MarkGUCPrefixReserved("plpgsql")`. Idempotent.
            pg_init: Some(builtin_pg_init),
        },
    );
}

/// `_PG_init`-equivalent run when `$libdir/plpgsql` is loaded via `LOAD`
/// (dfmgr's builtin-library path). Runs the custom-GUC registration slice
/// (`DefineCustom*Variable` + `MarkGUCPrefixReserved("plpgsql")`) once per
/// backend, matching C's `_PG_init` reserving the `plpgsql` prefix at load time.
fn builtin_pg_init() -> types_error::PgResult<()> {
    ensure_custom_gucs_registered();
    Ok(())
}

/// The array-iteration leg of `exec_stmt_foreach_a` (pl_exec.c). Given the
/// already-evaluated FOREACH array's verbatim varlena byte image, its runtime
/// array type/typmod, and the `slice` dimension, perform the C array + fmgr
/// substrate steps and materialize every element (`slice == 0`) or sub-array
/// (`slice > 0`) as a `ForeachItem` in iteration order:
///
/// ```c
/// if (!OidIsValid(get_element_type(arrtype)))
///     ereport(ERROR, ... "FOREACH expression must yield an array, not type %s" ...);
/// arr = DatumGetArrayTypePCopy(value);
/// if (stmt->slice < 0 || stmt->slice > ARR_NDIM(arr))
///     ereport(ERROR, ... "slice dimension (%d) is out of the valid range 0..%d" ...);
/// array_iterator = array_create_iterator(arr, stmt->slice, NULL);
/// if (stmt->slice > 0) { iterator_result_type = arrtype; ... }
/// else { iterator_result_type = ARR_ELEMTYPE(arr); ... }
/// while (array_iterate(array_iterator, &value, &isnull)) { ... }
/// ```
fn foreach_iterate_via_array_impl(
    arr_bytes: Vec<u8>,
    arrtype: types_core::Oid,
    arrtypmod: i32,
    slice: i32,
) -> PgResult<backend_pl_plpgsql_exec_seams::ForeachIterateResult> {
    use backend_pl_plpgsql_exec_seams::{ForeachItem, ForeachIterateResult};
    use backend_utils_adt_arrayfuncs::{foundation, sql::ArrayIterateItem};

    let ctx = mcx::MemoryContext::new("plpgsql FOREACH array");
    let mcx = ctx.mcx();

    // check the type of the expression - must be an array
    //   if (!OidIsValid(get_element_type(arrtype)))
    let elem_type =
        backend_utils_cache_lsyscache_seams::get_element_type::call(arrtype)?.unwrap_or(0);
    if elem_type == 0 {
        let tyname = backend_utils_adt_format_type_seams::format_type_be_owned::call(arrtype)
            .unwrap_or_else(|_| format!("type {arrtype}"));
        return Err(types_error::PgError::error(format!(
            "FOREACH expression must yield an array, not type {tyname}"
        ))
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH));
    }

    // arr = DatumGetArrayTypePCopy(value); — detoast the on-disk array image into
    // the working context. `array_create_iterator` reads the detoasted bytes.
    let arr = backend_access_common_detoast_seams::detoast_attr::call(mcx, &arr_bytes)?;
    let arr_bytes_detoasted: Vec<u8> = arr.as_slice().to_vec();
    let ndim = foundation::arr_ndim(&arr);

    // Slice dimension must be less than or equal to array dimension
    //   if (stmt->slice < 0 || stmt->slice > ARR_NDIM(arr))
    if slice < 0 || slice > ndim {
        return Err(types_error::PgError::error(format!(
            "slice dimension ({slice}) is out of the valid range 0..{ndim}"
        ))
        .with_sqlstate(types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    // Identify iterator result type
    //   if (stmt->slice > 0) { result = arrtype; } else { result = ARR_ELEMTYPE(arr); }
    let (result_type, result_typmod) = if slice > 0 {
        (arrtype, arrtypmod)
    } else {
        (foundation::arr_elemtype(&arr), arrtypmod)
    };

    // The element type metadata (typlen/typbyval/typalign) drives both
    // `array_create_iterator`'s fetch math and our by-ref element image
    // extraction below.
    let mstate = backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(elem_type)?;
    let elem_byval = mstate.typbyval;
    let elem_typlen = mstate.typlen;

    // array_iterator = array_create_iterator(arr, stmt->slice, NULL);
    let mut iterator = backend_utils_adt_arrayfuncs::sql::array_create_iterator(
        mcx,
        &arr_bytes_detoasted,
        slice,
        Some(mstate),
    )?;

    // while (array_iterate(array_iterator, &value, &isnull)) { ... }
    let mut items: Vec<ForeachItem> = Vec::new();
    while let Some(item) =
        backend_utils_adt_arrayfuncs::sql::array_iterate(mcx, &mut iterator)?
    {
        match item {
            ArrayIterateItem::Scalar { value, isnull } => {
                if isnull {
                    items.push(ForeachItem {
                        value: 0,
                        isnull: true,
                        byref: None,
                    });
                } else if elem_byval {
                    // A by-value element: the bare scalar word is the value.
                    items.push(ForeachItem {
                        value: value.as_usize(),
                        isnull: false,
                        byref: None,
                    });
                } else {
                    // A by-reference element (text/numeric/composite/…): in the
                    // byte model `array_iterate`'s `fetch_att` returns the element's
                    // in-buffer OFFSET (not a machine pointer), so slice the
                    // verbatim element image out of the detoasted array buffer,
                    // bounded by the element's length convention (VARSIZE_ANY for a
                    // varlena `typlen == -1`, strlen for a cstring `typlen == -2`,
                    // `typlen` bytes for a fixed-length-by-ref type). The image
                    // rides the out-of-band by-ref companion (bare word is 0 then).
                    let off = value.as_usize();
                    let img =
                        foreach_byref_element_image(&arr_bytes_detoasted, off, elem_typlen);
                    items.push(ForeachItem {
                        value: 0,
                        isnull: false,
                        byref: Some(img),
                    });
                }
            }
            ArrayIterateItem::Slice(bytes) => {
                // A freshly built sub-array (slice case): the result is always a
                // pass-by-reference array varlena. Carry its verbatim image.
                items.push(ForeachItem {
                    value: 0,
                    isnull: false,
                    byref: Some(bytes.as_slice().to_vec()),
                });
            }
        }
    }

    Ok(ForeachIterateResult {
        items,
        result_type,
        result_typmod,
    })
}

/// Copy a by-reference array element's verbatim byte image out of the detoasted
/// array `buf` at in-buffer offset `off`. The element's length follows its
/// `typlen` convention (mirroring `att_addlength_pointer` / fetch_att's by-ref
/// arm): `typlen == -1` is a varlena (`VARSIZE_ANY`), `typlen == -2` is a
/// NUL-terminated cstring, and `typlen > 0` is a fixed-length-by-ref blob of
/// `typlen` bytes. All reads are bounds-clamped against the buffer (a
/// well-formed array never overruns, but the clamp keeps this safe).
fn foreach_byref_element_image(buf: &[u8], off: usize, typlen: i16) -> Vec<u8> {
    if off >= buf.len() {
        return Vec::new();
    }
    let total = if typlen == -1 {
        // VARSIZE_ANY(DatumGetPointer(src)).
        backend_utils_adt_arrayfuncs::foundation::varsize_any(buf, off)
    } else if typlen == -2 {
        // strlen(cstring) + 1 (include the terminating NUL).
        buf[off..]
            .iter()
            .position(|&b| b == 0)
            .map(|n| n + 1)
            .unwrap_or(buf.len() - off)
    } else {
        // Fixed-length-by-ref: exactly `typlen` bytes.
        typlen.max(0) as usize
    };
    let end = (off + total).min(buf.len());
    buf[off..end].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The identifier-list branch routes through `split_identifier_string` ->
    // `downcase_identifier` (scansup seam), uninstalled in a bare unit test; the
    // "all"/"none" fast paths do not touch it, so we test those (the list branch
    // is exercised end-to-end once seams are installed).
    #[test]
    fn extra_checks_all_and_none() {
        let ctx = mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        assert_eq!(
            plpgsql_extra_checks_check_hook(mcx, "all").unwrap(),
            Ok(PLPGSQL_XCHECK_ALL)
        );
        assert_eq!(
            plpgsql_extra_checks_check_hook(mcx, "ALL").unwrap(),
            Ok(PLPGSQL_XCHECK_ALL)
        );
        assert_eq!(
            plpgsql_extra_checks_check_hook(mcx, "none").unwrap(),
            Ok(PLPGSQL_XCHECK_NONE)
        );
    }

    #[test]
    fn assign_hooks_set_globals() {
        plpgsql_extra_warnings_assign_hook(PLPGSQL_XCHECK_SHADOWVAR);
        assert_eq!(plpgsql_extra_warnings(), PLPGSQL_XCHECK_SHADOWVAR);
        plpgsql_extra_errors_assign_hook(PLPGSQL_XCHECK_TOOMANYROWS);
        assert_eq!(plpgsql_extra_errors(), PLPGSQL_XCHECK_TOOMANYROWS);
        plpgsql_extra_warnings_assign_hook(PLPGSQL_XCHECK_NONE);
        plpgsql_extra_errors_assign_hook(PLPGSQL_XCHECK_NONE);
    }

    #[test]
    fn guc_defaults_match_c() {
        assert_eq!(
            plpgsql_variable_conflict(),
            PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR
        );
        assert!(!plpgsql_print_strict_params());
        assert!(plpgsql_check_asserts());
        assert_eq!(plpgsql_extra_warnings(), PLPGSQL_XCHECK_NONE);
        assert_eq!(plpgsql_extra_errors(), PLPGSQL_XCHECK_NONE);
    }
}
