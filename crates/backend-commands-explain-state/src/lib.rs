#![allow(non_snake_case)]
#![forbid(unsafe_code)]
// `PgError` is the large shared error type used across the whole tree; boxing it
// here would diverge from every sibling crate's `PgResult` shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/explain_state.c` — code for initializing and accessing
//! `ExplainState` objects (PostgreSQL 18.3).
//!
//! In-core EXPLAIN options have hard-coded fields inside [`ExplainState`]; e.g.
//! `EXPLAIN (BUFFERS)` sets `es.buffers`. Extensions register options via
//! [`register_extension_explain_option`] and store private state in the
//! `extension_state` slots via [`set_explain_extension_state`] /
//! [`get_explain_extension_state`], keyed by the id
//! [`get_explain_extension_id`] hands out.
//!
//! ## Per-backend registry
//!
//! The C file-scope `static` globals (`ExplainExtensionNameArray`,
//! `ExplainExtensionOptionArray`, and `explain_validate_options_hook`) are
//! per-backend mutable state (PostgreSQL is process-per-backend). They are
//! `MemoryContextAlloc(TopMemoryContext, ...)`-allocated, i.e. backend-lifetime
//! globals; per `docs/mctx-design.md` decision 5 such state is modeled as a
//! `thread_local!` `RefCell` holding plain owned `String`/`Vec`, not an
//! mcx-charged value. The C `assigned`/`allocated` split with `pg_nextpower2_32`
//! growth becomes `Vec::len()`/`Vec`'s own capacity; the observable behavior
//! (name→id mapping, register-or-update by name, hook invocation) is identical.
//!
//! ## Seams
//!
//! * `defGetBoolean` / `defGetString` (define.c) are called directly on
//!   [`backend_commands_define`] (no cycle — this crate is a leaf).
//! * `parser_errposition` (parse_node.c) crosses the
//!   [`backend_parser_small1_seams`] owner seam.
//! * The extension callbacks (`ExplainOptionHandler`, the
//!   `explain_validate_options_hook`) are foreign extension function pointers;
//!   invoking one crosses the
//!   [`backend_commands_explain_state_seams`] `call_*` seam, which panics until
//!   an extension installs it. The in-core dispatch never reaches them (no
//!   handler can be registered without an extension), so the panic path is
//!   correct mirror-PG behavior.

use core::cell::RefCell;

use backend_utils_error::ereport;
use mcx::Mcx;
use types_cluster::ParseState;
use types_error::{
    PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_explain::{ExplainFormat, ExplainSerializeOption, ExplainState, ExtensionStateHandle};
use types_parsenodes::DefElem;

use backend_commands_define::{defGetBoolean, defGetString};
use backend_commands_explain_state_seams as seam;
use backend_commands_explain_state_seams::{ExplainOptionHandler, ExplainValidateOptionsHook};
use backend_parser_small1_seams::parser_errposition;

/// `pg_nextpower2_32(num)` (port/pg_bitutils.h) — round `num` up to the next
/// power of two. `num` must be `> 0` and `<= PG_UINT32_MAX / 2 + 1`.
#[inline]
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0 && num <= u32::MAX / 2 + 1);

    // A power-2 number has only one bit set; subtracting 1 turns on all lower
    // bits, so `num & (num - 1) == 0` iff already a power of two.
    if (num & (num - 1)) == 0 {
        return num;
    }
    // (uint32) 1 << (pg_leftmost_one_pos32(num) + 1);
    // pg_leftmost_one_pos32(num) == 31 - clz(num) for num != 0.
    1u32 << (pg_leftmost_one_pos32(num) + 1)
}

/// `pg_leftmost_one_pos32(word)` (port/pg_bitutils.h) — the 0-based position of
/// the most-significant set bit. `word` must be non-zero.
#[inline]
fn pg_leftmost_one_pos32(word: u32) -> u32 {
    debug_assert!(word != 0);
    31 - word.leading_zeros()
}

/// `Max(a, b)` — the C macro.
#[inline]
fn max_i32(a: i32, b: i32) -> i32 {
    if a > b {
        a
    } else {
        b
    }
}

// ===========================================================================
// File-scope per-backend registry (explain_state.c file statics).
// ===========================================================================
//
// `MemoryContextAlloc(TopMemoryContext, ...)` => backend-lifetime globals,
// modeled as a thread_local of plain owned values.

/// One registered extension EXPLAIN option — the file-scope
/// `typedef struct { const char *option_name; ExplainOptionHandler option_handler; }`.
struct ExplainExtensionOption {
    /// `const char *option_name`.
    option_name: String,
    /// `ExplainOptionHandler option_handler`.
    option_handler: ExplainOptionHandler,
}

/// The file-scope `static` globals of `explain_state.c`, gathered per backend.
#[derive(Default)]
struct Registry {
    /// `static const char **ExplainExtensionNameArray` (+ the `Assigned` count,
    /// which is `len()`).
    extension_names: Vec<String>,
    /// `static ExplainExtensionOption *ExplainExtensionOptionArray` (+ the
    /// `Assigned` count, which is `len()`).
    extension_options: Vec<ExplainExtensionOption>,
    /// `explain_validate_options_hook_type explain_validate_options_hook = NULL;`
    validate_options_hook: Option<ExplainValidateOptionsHook>,
}

thread_local! {
    static REGISTRY: RefCell<Registry> = RefCell::new(Registry::default());
}

/// Set (or clear) the plugin `explain_validate_options_hook`.
///
/// The C global is directly assignable by loaded modules; this setter lets an
/// extension register the hook (as the opaque [`ExplainValidateOptionsHook`]
/// handle) without touching the per-backend state directly.
pub fn set_explain_validate_options_hook(hook: Option<ExplainValidateOptionsHook>) {
    REGISTRY.with(|r| r.borrow_mut().validate_options_hook = hook);
}

// ===========================================================================
// explain_state.c — ported functions.
// ===========================================================================

/// `NewExplainState(void)` (explain_state.c:61) — create a new `ExplainState`
/// struct initialized with default options.
///
/// The C `palloc0`s the struct and `makeStringInfo()`s the output buffer; the
/// idiomatic equivalent builds the value in `mcx`: every field defaults to its
/// zero/empty value ([`ExplainState::new_in`]), then `costs` is set true. The
/// output buffer `str` is the already-empty [`PgString`] `new_in` creates
/// (`makeStringInfo` starts empty).
pub fn NewExplainState<'mcx>(mcx: Mcx<'mcx>) -> ExplainState<'mcx> {
    let mut es = ExplainState::new_in(mcx);

    // Set default options (most fields can be left as zeroes).
    es.costs = true;
    // Prepare output buffer — new_in already created an empty PgString in mcx.

    es
}

/// `ParseExplainOptionList(es, options, pstate)` (explain_state.c:77) — parse a
/// list of EXPLAIN options and update an `ExplainState` accordingly.
pub fn ParseExplainOptionList<'mcx>(
    es: &mut ExplainState<'mcx>,
    options: &[DefElem],
    pstate: &mut ParseState,
) -> PgResult<()> {
    let mut timing_set = false;
    let mut buffers_set = false;
    let mut summary_set = false;

    // Parse options list.
    for opt in options {
        let defname = defname_str(opt);
        if defname == "analyze" {
            es.analyze = defGetBoolean(opt)?;
        } else if defname == "verbose" {
            es.verbose = defGetBoolean(opt)?;
        } else if defname == "costs" {
            es.costs = defGetBoolean(opt)?;
        } else if defname == "buffers" {
            buffers_set = true;
            es.buffers = defGetBoolean(opt)?;
        } else if defname == "wal" {
            es.wal = defGetBoolean(opt)?;
        } else if defname == "settings" {
            es.settings = defGetBoolean(opt)?;
        } else if defname == "generic_plan" {
            es.generic = defGetBoolean(opt)?;
        } else if defname == "timing" {
            timing_set = true;
            es.timing = defGetBoolean(opt)?;
        } else if defname == "summary" {
            summary_set = true;
            es.summary = defGetBoolean(opt)?;
        } else if defname == "memory" {
            es.memory = defGetBoolean(opt)?;
        } else if defname == "serialize" {
            if opt.arg.is_some() {
                let mcx = es.str.allocator();
                let p = defGetString(mcx, opt)?;
                let p = p.as_str();

                if p == "off" || p == "none" {
                    es.serialize = ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE;
                } else if p == "text" {
                    es.serialize = ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT;
                } else if p == "binary" {
                    es.serialize = ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY;
                } else {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!(
                            "unrecognized value for {} option \"{}\": \"{}\"",
                            "EXPLAIN", defname, p
                        ))
                        .errposition(parser_errposition::call(pstate, opt.location)?)
                        .into_error());
                }
            } else {
                // SERIALIZE without an argument is taken as 'text'.
                es.serialize = ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT;
            }
        } else if defname == "format" {
            let mcx = es.str.allocator();
            let p = defGetString(mcx, opt)?;
            let p = p.as_str();

            if p == "text" {
                es.format = ExplainFormat::EXPLAIN_FORMAT_TEXT;
            } else if p == "xml" {
                es.format = ExplainFormat::EXPLAIN_FORMAT_XML;
            } else if p == "json" {
                es.format = ExplainFormat::EXPLAIN_FORMAT_JSON;
            } else if p == "yaml" {
                es.format = ExplainFormat::EXPLAIN_FORMAT_YAML;
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "unrecognized value for {} option \"{}\": \"{}\"",
                        "EXPLAIN", defname, p
                    ))
                    .errposition(parser_errposition::call(pstate, opt.location)?)
                    .into_error());
            }
        } else if !ApplyExtensionExplainOption(es, opt, pstate)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized {} option \"{}\"", "EXPLAIN", defname))
                .errposition(parser_errposition::call(pstate, opt.location)?)
                .into_error());
        }
    }

    // check that WAL is used with EXPLAIN ANALYZE
    if es.wal && !es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("EXPLAIN option {} requires ANALYZE", "WAL"))
            .into_error());
    }

    // if the timing was not set explicitly, set default value
    es.timing = if timing_set { es.timing } else { es.analyze };

    // if the buffers was not set explicitly, set default value
    es.buffers = if buffers_set { es.buffers } else { es.analyze };

    // check that timing is used with EXPLAIN ANALYZE
    if es.timing && !es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("EXPLAIN option {} requires ANALYZE", "TIMING"))
            .into_error());
    }

    // check that serialize is used with EXPLAIN ANALYZE
    if es.serialize != ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE && !es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("EXPLAIN option {} requires ANALYZE", "SERIALIZE"))
            .into_error());
    }

    // check that GENERIC_PLAN is not used with EXPLAIN ANALYZE
    if es.generic && es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "{} options {} and {} cannot be used together",
                "EXPLAIN", "ANALYZE", "GENERIC_PLAN"
            ))
            .into_error());
    }

    // if the summary was not set explicitly, set default value
    es.summary = if summary_set { es.summary } else { es.analyze };

    // plugin specific option validation
    let hook = REGISTRY.with(|r| r.borrow().validate_options_hook);
    if let Some(hook) = hook {
        seam::call_validate_options_hook::call(hook, es, options, pstate)?;
    }

    Ok(())
}

/// `GetExplainExtensionId(extension_name)` (explain_state.c:221) — map the name
/// of an EXPLAIN extension to an integer ID.
///
/// Within the lifetime of a particular backend, the same name maps to the same
/// ID every time. IDs are not stable across backends.
pub fn GetExplainExtensionId(extension_name: &str) -> i32 {
    REGISTRY.with(|r| {
        let mut reg = r.borrow_mut();

        // Search for an existing extension by this name; if found, return ID.
        for (i, name) in reg.extension_names.iter().enumerate() {
            if name == extension_name {
                return i as i32;
            }
        }

        // Assign and return new ID. The C's manual assigned/allocated split
        // with pg_nextpower2_32 growth (a TopMemoryContext alloc that elog's on
        // OOM) is the backend-lifetime Vec's own growth here.
        let id = reg.extension_names.len() as i32;
        reg.extension_names.push(extension_name.to_owned());
        id
    })
}

/// `GetExplainExtensionState(es, extension_id)` (explain_state.c:259) — get
/// extension-specific state from an `ExplainState`.
pub fn GetExplainExtensionState(
    es: &ExplainState<'_>,
    extension_id: i32,
) -> Option<ExtensionStateHandle> {
    debug_assert!(extension_id >= 0); // Assert(extension_id >= 0);

    if extension_id >= es.extension_state_allocated {
        return None;
    }

    // return es->extension_state[extension_id];  (the stored void *, NULL=None)
    es.extension_state
        .get(extension_id as usize)
        .copied()
        .flatten()
}

/// `SetExplainExtensionState(es, extension_id, opaque)` (explain_state.c:278) —
/// store extension-specific state into an `ExplainState`.
///
/// `opaque` is the value to store (`None` clears the slot, equivalent to the C
/// `NULL`).
pub fn SetExplainExtensionState(
    es: &mut ExplainState<'_>,
    extension_id: i32,
    opaque: Option<ExtensionStateHandle>,
) -> PgResult<()> {
    debug_assert!(extension_id >= 0); // Assert(extension_id >= 0);

    // If there is no array yet, create one.
    if es.extension_state.is_empty() {
        // Max(16, pg_nextpower2_32(extension_id + 1))
        es.extension_state_allocated =
            max_i32(16, pg_nextpower2_32((extension_id + 1) as u32) as i32);
        grow_extension_state(es, es.extension_state_allocated as usize)?;
    }

    // If there's an array but it's currently full, expand it.
    if extension_id >= es.extension_state_allocated {
        let i = pg_nextpower2_32((extension_id + 1) as u32) as i32;
        grow_extension_state(es, i as usize)?;
        es.extension_state_allocated = i;
    }

    es.extension_state[extension_id as usize] = opaque;
    Ok(())
}

/// Grow `es.extension_state` to `want` `None` slots (the C `palloc0` /
/// `repalloc0` zero-fill), charged to the `ExplainState`'s context. A failed
/// reservation degrades to the context's out-of-memory error, mirroring a
/// `palloc`/`repalloc` OOM.
fn grow_extension_state(es: &mut ExplainState<'_>, want: usize) -> PgResult<()> {
    let mcx = es.extension_state.allocator();
    let cur = es.extension_state.len();
    if want > cur {
        let additional = want - cur;
        let oom = mcx.oom(additional * core::mem::size_of::<Option<ExtensionStateHandle>>());
        es.extension_state.try_reserve(additional).map_err(|_| oom)?;
        es.extension_state.resize(want, None);
    }
    Ok(())
}

/// `RegisterExtensionExplainOption(option_name, handler)` (explain_state.c:318)
/// — register a new EXPLAIN option.
///
/// `option_name` is assumed to be a constant string or allocated in storage that
/// will never be freed (here: an owned `String` in the backend-lifetime
/// registry).
pub fn RegisterExtensionExplainOption(option_name: &str, handler: ExplainOptionHandler) {
    REGISTRY.with(|r| {
        let mut reg = r.borrow_mut();

        // Search for an existing option by this name; if found, update handler.
        for exopt in reg.extension_options.iter_mut() {
            if exopt.option_name == option_name {
                exopt.option_handler = handler;
                return;
            }
        }

        // Assign new option (the C's pg_nextpower2_32 array growth is the
        // backend-lifetime Vec's own).
        reg.extension_options.push(ExplainExtensionOption {
            option_name: option_name.to_owned(),
            option_handler: handler,
        });
    });
}

/// `ApplyExtensionExplainOption(es, opt, pstate)` (explain_state.c:367) — apply
/// an EXPLAIN option registered by an extension.
///
/// If no extension has registered the named option, returns `false`. Otherwise,
/// calls the appropriate handler function and then returns `true`.
pub fn ApplyExtensionExplainOption<'mcx>(
    es: &mut ExplainState<'mcx>,
    opt: &DefElem,
    pstate: &mut ParseState,
) -> PgResult<bool> {
    let defname = defname_str(opt);

    // Look the handler up without holding the registry borrow across the seam
    // call (the foreign handler may re-enter the registry).
    let handler = REGISTRY.with(|r| {
        r.borrow()
            .extension_options
            .iter()
            .find(|exopt| exopt.option_name == defname)
            .map(|exopt| exopt.option_handler)
    });

    match handler {
        Some(handler) => {
            seam::call_option_handler::call(handler, es, opt, pstate)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// This crate owns no inward seam that another ported crate installs: the
/// `call_option_handler` / `call_validate_options_hook` seams in
/// `backend-commands-explain-state-seams` are foreign extension callbacks, set
/// by an extension when it lands (none is ported), so this installer is empty —
/// mirroring `commands/user.c`'s empty `init_seams()` for its own hook-call
/// seams.
pub fn init_seams() {}

// ===========================================================================
// small in-crate helpers
// ===========================================================================

/// Borrow `opt->defname` (a `char *` in C) as a `&str` for the option dispatch.
/// The C compares with `strcmp`; a NULL `defname` (modeled as `None`) is treated
/// as the empty string, so no in-core option matches it and it falls through to
/// the extension lookup / "unrecognized option" error.
#[inline]
fn defname_str(opt: &DefElem) -> &str {
    opt.defname.as_deref().unwrap_or("")
}

#[cfg(test)]
mod tests;
