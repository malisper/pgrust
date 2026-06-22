//! `commands/copy.c` — the COPY utility command.
//!
//! This crate owns the COPY *driver* entry `DoCopy` and the shared option half
//! (`ProcessCopyOptions` / `CopyGetAttnums` + the `defGetCopy*` value
//! extractors), and installs the three inward seams the rest of the tree calls:
//!
//!   * `backend-commands-copy-seams::process_copy_options` / `copy_get_attnums`
//!     — called by both COPY drivers (copyto / copyfrom).
//!   * `backend-tcop-utility-out-seams::do_copy` — the utility dispatcher's
//!     `T_CopyStmt` entry.
//!
//! `DoCopy` dispatches COPY TO → `DoCopyTo` (backend-commands-copyto, fully
//! ported, reached directly) and COPY FROM → `BeginCopyFrom`/`CopyFrom`
//! (backend-commands-copyfrom, not yet ported — that leg panics until it
//! lands).
//!
//! `DefElem` value extraction mirrors `commands/collationcmds.c`: a parse-tree
//! `Node::DefElem`'s `arg` is projected into the `DefElemArg` the `define.c`
//! value accessors switch on, then `def_get_string` / `def_get_boolean` are
//! called through `backend-commands-define-seams`.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};

use mcx::{Mcx, PgString, PgVec};
use types_acl::acl::{CheckEnableRlsResult, ACL_INSERT, ACL_SELECT};
use types_catalog::catalog::{
    ROLE_PG_EXECUTE_SERVER_PROGRAM, ROLE_PG_READ_SERVER_FILES, ROLE_PG_WRITE_SERVER_FILES,
};
use types_copy::{
    CopyFormatOptions, CopyHeaderChoice, CopyLogVerbosityChoice, CopyOnErrorChoice,
};
use types_core::primitive::{AttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_nodes::ddlnodes::{CopyStmt, DefElem};
use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::nodelimit::LimitOption;
use types_nodes::parsestmt::{ParseState, RawStmt};
use types_nodes::rawnodes::{ColumnRef, ResTarget, SelectStmt, SetOperation};
use types_nodes::Expr;
use types_tuple::access::RELPERSISTENCE_TEMP;
use types_tuple::heaptuple::{TupleDesc, FirstLowInvalidHeapAttributeNumber};

// Seam aliases.
use backend_catalog_aclchk_seams as aclchk_s;
use backend_commands_define_seams as define_s;
use backend_commands_define_seams::DefElemArg;
use backend_parser_small1_seams as small1_s;

// SQLSTATE constants.
use types_error::{
    ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_COLUMN,
};

// ===========================================================================
// DefElem value extraction (mirrors collationcmds.c / define.c).
// ===========================================================================

/// `def->defname` — the option name, used in error messages.
fn def_name<'a>(defel: &'a DefElem) -> &'a str {
    defel.defname.as_deref().unwrap_or("")
}

/// Project a parse-tree `DefElem`'s value node into the `DefElemArg` the
/// `define.c` value accessors switch on. `None` for `def->arg == NULL`.
fn defel_arg(defel: &DefElem) -> PgResult<Option<DefElemArg>> {
    let Some(node) = defel.arg.as_deref() else {
        return Ok(None);
    };
    // Mirror `defGetString`'s full node switch (define.c): a bare-identifier
    // option value arrives as a `T_TypeName` and a qualified name as a
    // `T_List`; both render to text. A `_ => AStar` catch-all would collapse
    // those to `"*"`.
    Ok(Some(match node.node_tag() {
        ntag::T_Integer => DefElemArg::Integer(node.expect_integer().ival as i64),
        ntag::T_Float => DefElemArg::Float(node.expect_float().fval.to_string()),
        ntag::T_Boolean => DefElemArg::Boolean(node.expect_boolean().boolval),
        ntag::T_String => DefElemArg::String(node.expect_string().sval.to_string()),
        ntag::T_TypeName => DefElemArg::TypeName(defel_type_name_to_string(node.expect_typename())?),
        ntag::T_List => DefElemArg::List(defel_name_list_to_string(node.expect_list())?),
        ntag::T_A_Star => DefElemArg::AStar,
        other => {
            return Err(PgError::error(format!("unrecognized node type: {}", other))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR))
        }
    }))
}

/// `TypeNameToString(typeName)` for the `defGetString` `T_TypeName` case.
fn defel_type_name_to_string(tn: &types_nodes::rawnodes::TypeName<'_>) -> PgResult<String> {
    if tn.names.is_empty() {
        return Err(PgError::error("DefElem TypeName carries no name")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    let mut out = String::new();
    for (i, name) in tn.names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            other => {
                return Err(PgError::error(format!("unrecognized node type: {}", other))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR))
            }
        }
    }
    if tn.pct_type {
        out.push_str("%TYPE");
    }
    if !tn.arrayBounds.is_empty() {
        out.push_str("[]");
    }
    Ok(out)
}

/// `NameListToString(names)` (namespace.c) for the `defGetString` `T_List` case.
fn defel_name_list_to_string(names: &[NodePtr<'_>]) -> PgResult<String> {
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            ntag::T_A_Star => out.push('*'),
            other => {
                return Err(PgError::error(format!("unrecognized node type: {}", other))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR))
            }
        }
    }
    Ok(out)
}

/// `defGetString(def)` (define.c) — render the value as a string (`mcx`-owned).
fn def_get_string<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem) -> PgResult<String> {
    let s = define_s::def_get_string::call(
        mcx,
        def_name(defel).to_string(),
        defel_arg(defel)?,
    )?;
    Ok(s.to_string())
}

/// `defGetBoolean(def)` (define.c).
fn def_get_boolean(defel: &DefElem) -> PgResult<bool> {
    define_s::def_get_boolean::call(
        def_name(defel).to_string(),
        defel_arg(defel)?,
    )
}

/// `errorConflictingDefElem(defel, pstate)` (aclchk.c) — always raises.
fn error_conflicting_def_elem(defel: &DefElem) -> PgError {
    match aclchk_s::error_conflicting_def_elem::call(def_name(defel).to_string()) {
        Err(e) => e,
        // The seam always returns Err; synthesise the message if it somehow
        // does not (keeps the C "always raises" contract).
        Ok(()) => PgError::error(format!("conflicting or redundant options"))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    }
}

/// `parser_errposition(pstate, location)` — 0 when `pstate` is `None`.
fn errpos(pstate: Option<&ParseState<'_>>, location: i32) -> i32 {
    match pstate {
        Some(p) => small1_s::parser_errposition::call(p, location).unwrap_or(0),
        None => 0,
    }
}

// ---------------------------------------------------------------------------
// defGetCopyHeaderChoice (copy.c:368)
// ---------------------------------------------------------------------------

/// Extract a `CopyHeaderChoice` from a `DefElem`. Like `defGetBoolean` but also
/// accepts `"match"`.
fn defGetCopyHeaderChoice<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem, is_from: bool) -> PgResult<CopyHeaderChoice> {
    // If no parameter value given, assume "true" is meant.
    let Some(arg) = defel.arg.as_deref() else {
        return Ok(CopyHeaderChoice::COPY_HEADER_TRUE);
    };

    // Allow 0, 1, "true", "false", "on", "off", or "match".
    if arg.node_tag() == ntag::T_Integer {
        let i = arg.expect_integer();
        match i.ival {
            0 => return Ok(CopyHeaderChoice::COPY_HEADER_FALSE),
            1 => return Ok(CopyHeaderChoice::COPY_HEADER_TRUE),
            _ => {}
        }
    } else {
        let sval = def_get_string(mcx, defel)?;
        if pg_strcasecmp(&sval, "true") {
            return Ok(CopyHeaderChoice::COPY_HEADER_TRUE);
        }
        if pg_strcasecmp(&sval, "false") {
            return Ok(CopyHeaderChoice::COPY_HEADER_FALSE);
        }
        if pg_strcasecmp(&sval, "on") {
            return Ok(CopyHeaderChoice::COPY_HEADER_TRUE);
        }
        if pg_strcasecmp(&sval, "off") {
            return Ok(CopyHeaderChoice::COPY_HEADER_FALSE);
        }
        if pg_strcasecmp(&sval, "match") {
            if !is_from {
                return Err(PgError::error(format!(
                    "cannot use \"{sval}\" with HEADER in COPY TO"
                ))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            return Ok(CopyHeaderChoice::COPY_HEADER_MATCH);
        }
    }

    Err(PgError::error(format!(
        "{} requires a Boolean value or \"match\"",
        def_name(defel)
    ))
    .with_sqlstate(ERRCODE_SYNTAX_ERROR))
}

// ---------------------------------------------------------------------------
// defGetCopyOnErrorChoice (copy.c:432)
// ---------------------------------------------------------------------------

fn defGetCopyOnErrorChoice<'mcx>(
    mcx: Mcx<'mcx>,
    defel: &DefElem,
    pstate: Option<&ParseState<'_>>,
    is_from: bool,
) -> PgResult<CopyOnErrorChoice> {
    let sval = def_get_string(mcx, defel)?;

    if !is_from {
        return Err(PgError::error(format!("COPY {} cannot be used with {}", "ON_ERROR", "COPY TO"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_cursor_position(errpos(pstate, defel.location)));
    }

    if pg_strcasecmp(&sval, "stop") {
        return Ok(CopyOnErrorChoice::COPY_ON_ERROR_STOP);
    }
    if pg_strcasecmp(&sval, "ignore") {
        return Ok(CopyOnErrorChoice::COPY_ON_ERROR_IGNORE);
    }

    Err(PgError::error(format!("COPY {} \"{}\" not recognized", "ON_ERROR", sval))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .with_cursor_position(errpos(pstate, defel.location)))
}

// ---------------------------------------------------------------------------
// defGetCopyRejectLimitOption (copy.c:467)
// ---------------------------------------------------------------------------

fn defGetCopyRejectLimitOption(defel: &DefElem) -> PgResult<i64> {
    let reject_limit = match defel.arg.as_deref() {
        None => {
            return Err(PgError::error(format!("{} requires a numeric value", def_name(defel)))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR));
        }
        Some(s) if s.node_tag() == ntag::T_String => backend_utils_adt_numutils::pg_strtoint64(s.expect_string().sval.as_str())?,
        Some(i) if i.node_tag() == ntag::T_Integer => i.expect_integer().ival as i64,
        Some(_) => {
            // defGetInt64 over the projected arg: only Integer / Float / String
            // produce an int64; anything else errors like define.c.
            match defel_arg(defel)? {
                Some(DefElemArg::Integer(v)) => v,
                Some(DefElemArg::Float(s)) | Some(DefElemArg::String(s)) => {
                    backend_utils_adt_numutils::pg_strtoint64(&s)?
                }
                _ => {
                    return Err(PgError::error(format!(
                        "{} requires a numeric value",
                        def_name(defel)
                    ))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR));
                }
            }
        }
    };

    if reject_limit <= 0 {
        return Err(PgError::error(format!(
            "REJECT_LIMIT ({reject_limit}) must be greater than zero"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    Ok(reject_limit)
}

// ---------------------------------------------------------------------------
// defGetCopyLogVerbosityChoice (copy.c:495)
// ---------------------------------------------------------------------------

fn defGetCopyLogVerbosityChoice<'mcx>(
    mcx: Mcx<'mcx>,
    defel: &DefElem,
    pstate: Option<&ParseState<'_>>,
) -> PgResult<CopyLogVerbosityChoice> {
    let sval = def_get_string(mcx, defel)?;
    if pg_strcasecmp(&sval, "silent") {
        return Ok(CopyLogVerbosityChoice::COPY_LOG_VERBOSITY_SILENT);
    }
    if pg_strcasecmp(&sval, "default") {
        return Ok(CopyLogVerbosityChoice::COPY_LOG_VERBOSITY_DEFAULT);
    }
    if pg_strcasecmp(&sval, "verbose") {
        return Ok(CopyLogVerbosityChoice::COPY_LOG_VERBOSITY_VERBOSE);
    }

    Err(PgError::error(format!("COPY {} \"{}\" not recognized", "LOG_VERBOSITY", sval))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .with_cursor_position(errpos(pstate, defel.location)))
}

// ===========================================================================
// ProcessCopyOptions (copy.c:534)
// ===========================================================================

/// A freshly-zeroed [`CopyFormatOptions`] — the C `palloc0`'d struct
/// `ProcessCopyOptions` fills in.
fn new_copy_format_options<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CopyFormatOptions<'mcx>> {
    Ok(CopyFormatOptions {
        file_encoding: 0,
        binary: false,
        freeze: false,
        csv_mode: false,
        header_line: CopyHeaderChoice::COPY_HEADER_FALSE,
        null_print: PgString::from_str_in("", mcx)?,
        null_print_len: 0,
        null_print_client: PgString::from_str_in("", mcx)?,
        default_print: None,
        default_print_len: 0,
        delim: 0,
        quote: 0,
        escape: 0,
        force_quote: None,
        force_quote_all: false,
        force_quote_flags: PgVec::new_in(mcx),
        force_notnull: None,
        force_notnull_all: false,
        force_notnull_flags: PgVec::new_in(mcx),
        force_null: None,
        force_null_all: false,
        force_null_flags: PgVec::new_in(mcx),
        convert_selectively: false,
        on_error: CopyOnErrorChoice::COPY_ON_ERROR_STOP,
        log_verbosity: CopyLogVerbosityChoice::COPY_LOG_VERBOSITY_DEFAULT,
        reject_limit: 0,
        convert_select: None,
    })
}

/// `IsA(defel->arg, A_Star)`.
fn defel_arg_is_a_star(defel: &DefElem) -> bool {
    defel.arg.as_deref().is_some_and(|n| n.node_tag() == ntag::T_A_Star)
}

/// `IsA(defel->arg, List)` ⇒ the list's elements (column-name `String` nodes),
/// cloned into `mcx`. `None` when the arg is absent or not a `List`.
fn defel_arg_string_list<'mcx>(
    mcx: Mcx<'mcx>,
    defel: &DefElem,
) -> PgResult<Option<PgVec<'mcx, NodePtr<'mcx>>>> {
    let Some(arg) = defel.arg.as_deref() else {
        return Ok(None);
    };
    let Some(elems) = arg.as_list() else {
        return Ok(None);
    };
    let mut out: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve(elems.len()).map_err(|_| mcx.oom(elems.len()))?;
    for e in elems.iter() {
        out.push(mcx::alloc_in(mcx, e.clone_in(mcx)?)?);
    }
    Ok(Some(out))
}

/// Track the single-byte option (`delim`/`quote`/`escape`): the C stores a
/// `char *` (NULL = unset); the owned model stores the final `u8` (0 = unset).
/// A specified empty / multi-byte string is detected by the length checks
/// below; a specified value's first byte is what the codec reads.
#[derive(Default)]
struct OptStr {
    delim: Option<String>,
    null_print: Option<String>,
    default_print: Option<String>,
    quote: Option<String>,
    escape: Option<String>,
}

/// `ProcessCopyOptions(pstate, opts_out, is_from, options)` (copy.c:534): scan
/// the `DefElem` option list and transpose into a filled
/// `CopyFormatOptions`.
pub fn ProcessCopyOptions<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&ParseState<'mcx>>,
    is_from: bool,
    options: Option<&[NodePtr<'mcx>]>,
) -> PgResult<CopyFormatOptions<'mcx>> {
    let mut opts = new_copy_format_options(mcx)?;

    let mut format_specified = false;
    let mut freeze_specified = false;
    let mut header_specified = false;
    let mut on_error_specified = false;
    let mut log_verbosity_specified = false;
    let mut reject_limit_specified = false;

    // The string working set for delim/quote/escape/null/default — the C reads
    // these as `char *` while building, with a single-byte check at the end.
    let mut s = OptStr::default();

    opts.file_encoding = -1;

    // Extract options from the statement node tree.
    for node in options.unwrap_or(&[]) {
        let Some(defel) = node.as_ref().as_defelem() else {
            // lfirst_node(DefElem, option) — the parser only puts DefElems here.
            continue;
        };
        let dn = def_name(defel).to_string();
        let dn = dn.as_str();

        if dn == "format" {
            let fmt = def_get_string(mcx, defel)?;
            if format_specified {
                return Err(error_conflicting_def_elem(defel));
            }
            format_specified = true;
            if fmt == "text" {
                // default format
            } else if fmt == "csv" {
                opts.csv_mode = true;
            } else if fmt == "binary" {
                opts.binary = true;
            } else {
                return Err(PgError::error(format!("COPY format \"{fmt}\" not recognized"))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_cursor_position(errpos(pstate, defel.location)));
            }
        } else if dn == "freeze" {
            if freeze_specified {
                return Err(error_conflicting_def_elem(defel));
            }
            freeze_specified = true;
            opts.freeze = def_get_boolean(defel)?;
        } else if dn == "delimiter" {
            if s.delim.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            s.delim = Some(def_get_string(mcx, defel)?);
        } else if dn == "null" {
            if s.null_print.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            s.null_print = Some(def_get_string(mcx, defel)?);
        } else if dn == "default" {
            if s.default_print.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            s.default_print = Some(def_get_string(mcx, defel)?);
        } else if dn == "header" {
            if header_specified {
                return Err(error_conflicting_def_elem(defel));
            }
            header_specified = true;
            opts.header_line = defGetCopyHeaderChoice(mcx, defel, is_from)?;
        } else if dn == "quote" {
            if s.quote.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            s.quote = Some(def_get_string(mcx, defel)?);
        } else if dn == "escape" {
            if s.escape.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            s.escape = Some(def_get_string(mcx, defel)?);
        } else if dn == "force_quote" {
            if opts.force_quote.is_some() || opts.force_quote_all {
                return Err(error_conflicting_def_elem(defel));
            }
            if defel_arg_is_a_star(defel) {
                opts.force_quote_all = true;
            } else if let Some(names) = defel_arg_string_list(mcx, defel)? {
                opts.force_quote = Some(names);
            } else {
                return Err(force_list_error(dn, pstate, defel));
            }
        } else if dn == "force_not_null" {
            if opts.force_notnull.is_some() || opts.force_notnull_all {
                return Err(error_conflicting_def_elem(defel));
            }
            if defel_arg_is_a_star(defel) {
                opts.force_notnull_all = true;
            } else if let Some(names) = defel_arg_string_list(mcx, defel)? {
                opts.force_notnull = Some(names);
            } else {
                return Err(force_list_error(dn, pstate, defel));
            }
        } else if dn == "force_null" {
            if opts.force_null.is_some() || opts.force_null_all {
                return Err(error_conflicting_def_elem(defel));
            }
            if defel_arg_is_a_star(defel) {
                opts.force_null_all = true;
            } else if let Some(names) = defel_arg_string_list(mcx, defel)? {
                opts.force_null = Some(names);
            } else {
                return Err(force_list_error(dn, pstate, defel));
            }
        } else if dn == "convert_selectively" {
            // Undocumented, not-accessible-from-SQL option.
            if opts.convert_selectively {
                return Err(error_conflicting_def_elem(defel));
            }
            opts.convert_selectively = true;
            if defel.arg.is_none() {
                opts.convert_select = None;
            } else if let Some(names) = defel_arg_string_list(mcx, defel)? {
                opts.convert_select = Some(names);
            } else {
                return Err(force_list_error(dn, pstate, defel));
            }
        } else if dn == "encoding" {
            if opts.file_encoding >= 0 {
                return Err(error_conflicting_def_elem(defel));
            }
            let name = def_get_string(mcx, defel)?;
            opts.file_encoding = common_extra_encnames::pg_char_to_encoding(&name) as i32;
            if opts.file_encoding < 0 {
                return Err(PgError::error(format!(
                    "argument to option \"{dn}\" must be a valid encoding name"
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_cursor_position(errpos(pstate, defel.location)));
            }
        } else if dn == "on_error" {
            if on_error_specified {
                return Err(error_conflicting_def_elem(defel));
            }
            on_error_specified = true;
            opts.on_error = defGetCopyOnErrorChoice(mcx, defel, pstate, is_from)?;
        } else if dn == "log_verbosity" {
            if log_verbosity_specified {
                return Err(error_conflicting_def_elem(defel));
            }
            log_verbosity_specified = true;
            opts.log_verbosity = defGetCopyLogVerbosityChoice(mcx, defel, pstate)?;
        } else if dn == "reject_limit" {
            if reject_limit_specified {
                return Err(error_conflicting_def_elem(defel));
            }
            reject_limit_specified = true;
            opts.reject_limit = defGetCopyRejectLimitOption(defel)?;
        } else {
            return Err(PgError::error(format!("option \"{dn}\" not recognized"))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                .with_cursor_position(errpos(pstate, defel.location)));
        }
    }

    // Check for incompatible options (must do these three before defaults).
    if opts.binary && s.delim.is_some() {
        return Err(binary_mode_error("DELIMITER"));
    }
    if opts.binary && s.null_print.is_some() {
        return Err(binary_mode_error("NULL"));
    }
    if opts.binary && s.default_print.is_some() {
        return Err(binary_mode_error("DEFAULT"));
    }

    // Set defaults for omitted options.
    if s.delim.is_none() {
        s.delim = Some(if opts.csv_mode { "," } else { "\t" }.to_string());
    }
    if s.null_print.is_none() {
        s.null_print = Some(if opts.csv_mode { "" } else { "\\N" }.to_string());
    }
    let null_print = s.null_print.clone().unwrap();
    opts.null_print_len = null_print.len() as i32;

    if opts.csv_mode {
        if s.quote.is_none() {
            s.quote = Some("\"".to_string());
        }
        if s.escape.is_none() {
            s.escape = s.quote.clone();
        }
    }

    let delim = s.delim.clone().unwrap();

    // Only single-byte delimiter strings are supported.
    if delim.len() != 1 {
        return Err(PgError::error("COPY delimiter must be a single one-byte character")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // Disallow end-of-line characters.
    if delim.as_bytes().contains(&b'\r') || delim.as_bytes().contains(&b'\n') {
        return Err(PgError::error("COPY delimiter cannot be newline or carriage return")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    if null_print.as_bytes().contains(&b'\r') || null_print.as_bytes().contains(&b'\n') {
        return Err(PgError::error(
            "COPY null representation cannot use newline or carriage return",
        )
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    if let Some(dp) = s.default_print.clone() {
        opts.default_print_len = dp.len() as i32;
        if dp.as_bytes().contains(&b'\r') || dp.as_bytes().contains(&b'\n') {
            return Err(PgError::error(
                "COPY default representation cannot use newline or carriage return",
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }

    // Disallow unsafe delimiter characters in non-CSV mode.
    if !opts.csv_mode
        && b"\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(&delim.as_bytes()[0])
    {
        return Err(PgError::error(format!("COPY delimiter cannot be \"{delim}\""))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Check header.
    if opts.binary && opts.header_line != CopyHeaderChoice::COPY_HEADER_FALSE {
        return Err(binary_mode_feature_error("HEADER"));
    }

    // Check quote.
    if !opts.csv_mode && s.quote.is_some() {
        return Err(requires_csv_error("QUOTE"));
    }
    if opts.csv_mode && s.quote.as_deref().map(|q| q.len()).unwrap_or(0) != 1 {
        return Err(PgError::error("COPY quote must be a single one-byte character")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }
    let quote_byte = s.quote.as_deref().and_then(|q| q.as_bytes().first().copied()).unwrap_or(0);
    if opts.csv_mode && delim.as_bytes()[0] == quote_byte {
        return Err(PgError::error("COPY delimiter and quote must be different")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Check escape.
    if !opts.csv_mode && s.escape.is_some() {
        return Err(requires_csv_error("ESCAPE"));
    }
    if opts.csv_mode && s.escape.as_deref().map(|e| e.len()).unwrap_or(0) != 1 {
        return Err(PgError::error("COPY escape must be a single one-byte character")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // Check force_quote.
    if !opts.csv_mode && (opts.force_quote.is_some() || opts.force_quote_all) {
        return Err(requires_csv_error("FORCE_QUOTE"));
    }
    if (opts.force_quote.is_some() || opts.force_quote_all) && is_from {
        return Err(cannot_be_used_with("FORCE_QUOTE", "COPY FROM", ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // Check force_notnull.
    if !opts.csv_mode && (opts.force_notnull.is_some() || opts.force_notnull_all) {
        return Err(requires_csv_error("FORCE_NOT_NULL"));
    }
    if (opts.force_notnull.is_some() || opts.force_notnull_all) && !is_from {
        return Err(cannot_be_used_with("FORCE_NOT_NULL", "COPY TO", ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Check force_null.
    if !opts.csv_mode && (opts.force_null.is_some() || opts.force_null_all) {
        return Err(requires_csv_error("FORCE_NULL"));
    }
    if (opts.force_null.is_some() || opts.force_null_all) && !is_from {
        return Err(cannot_be_used_with("FORCE_NULL", "COPY TO", ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Don't allow the delimiter to appear in the null string.
    if null_print.as_bytes().contains(&delim.as_bytes()[0]) {
        return Err(PgError::error(format!(
            "COPY delimiter character must not appear in the {} specification",
            "NULL"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Don't allow the CSV quote char to appear in the null string.
    if opts.csv_mode && null_print.as_bytes().contains(&quote_byte) {
        return Err(PgError::error(format!(
            "CSV quote character must not appear in the {} specification",
            "NULL"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Check freeze.
    if opts.freeze && !is_from {
        return Err(cannot_be_used_with("FREEZE", "COPY TO", ERRCODE_INVALID_PARAMETER_VALUE));
    }

    if let Some(dp) = s.default_print.clone() {
        if !is_from {
            return Err(cannot_be_used_with("DEFAULT", "COPY TO", ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        // Don't allow the delimiter to appear in the default string.
        if dp.as_bytes().contains(&delim.as_bytes()[0]) {
            return Err(PgError::error(format!(
                "COPY delimiter character must not appear in the {} specification",
                "DEFAULT"
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        // Don't allow the CSV quote char to appear in the default string.
        if opts.csv_mode && dp.as_bytes().contains(&quote_byte) {
            return Err(PgError::error(format!(
                "CSV quote character must not appear in the {} specification",
                "DEFAULT"
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        // Don't allow the NULL and DEFAULT string to be the same.
        if opts.null_print_len == opts.default_print_len && null_print == dp {
            return Err(PgError::error(
                "NULL specification and DEFAULT specification cannot be the same",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }

    // Check on_error.
    if opts.binary && opts.on_error != CopyOnErrorChoice::COPY_ON_ERROR_STOP {
        return Err(PgError::error("only ON_ERROR STOP is allowed in BINARY mode")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    if opts.reject_limit != 0 && opts.on_error == CopyOnErrorChoice::COPY_ON_ERROR_STOP {
        return Err(PgError::error(format!(
            "COPY {} requires {} to be set to {}",
            "REJECT_LIMIT", "ON_ERROR", "IGNORE"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Commit the resolved single-byte / marker strings into the owned struct.
    opts.delim = delim.as_bytes()[0];
    opts.quote = quote_byte;
    opts.escape = s.escape.as_deref().and_then(|e| e.as_bytes().first().copied()).unwrap_or(0);
    opts.null_print = PgString::from_str_in(&null_print, mcx)?;
    opts.null_print_client = PgString::from_str_in(&null_print, mcx)?;
    opts.default_print = match s.default_print {
        Some(dp) => Some(PgString::from_str_in(&dp, mcx)?),
        None => None,
    };

    Ok(opts)
}

// ===========================================================================
// CopyGetAttnums (copy.c:983)
// ===========================================================================

/// `CopyGetAttnums(tupDesc, rel, attnamelist)` (copy.c:983): build the integer
/// list of attnums to be copied. `attnamelist` is a `List *` of `String` nodes
/// (`None` ⇒ the default non-dropped, non-generated column list). `rel` only
/// supplies the relation name for error reports.
pub fn CopyGetAttnums<'mcx>(
    mcx: Mcx<'mcx>,
    tup_desc: &TupleDesc<'mcx>,
    rel: Option<&types_rel::Relation<'mcx>>,
    attnamelist: Option<&[NodePtr<'mcx>]>,
) -> PgResult<PgVec<'mcx, AttrNumber>> {
    let mut attnums: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);

    let td = tup_desc
        .as_ref()
        .expect("CopyGetAttnums: tuple descriptor must not be NULL");
    let natts = td.natts as usize;

    match attnamelist {
        None => {
            // Generate default column list.
            for i in 0..natts {
                let attr = td.compact_attr(i);
                if attr.attisdropped || attr.attgenerated {
                    continue;
                }
                attnums.push((i + 1) as AttrNumber);
            }
        }
        Some(names) => {
            for name_node in names {
                let Some(sn) = name_node.as_ref().as_string() else {
                    // strVal(lfirst(l)) — the parser only puts String nodes here.
                    continue;
                };
                let name = sn.sval.as_str();

                // Lookup column name.
                let mut attnum: AttrNumber = 0; // InvalidAttrNumber
                for i in 0..natts {
                    let att = td.attr(i);
                    if att.attisdropped {
                        continue;
                    }
                    if att.attname.name_str() == name.as_bytes() {
                        if att.attgenerated != 0 {
                            return Err(PgError::error(format!(
                                "column \"{name}\" is a generated column"
                            ))
                            .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE)
                            .with_detail("Generated columns cannot be used in COPY."));
                        }
                        attnum = att.attnum;
                        break;
                    }
                }
                if attnum == 0 {
                    return Err(match rel {
                        Some(r) => PgError::error(format!(
                            "column \"{}\" of relation \"{}\" does not exist",
                            name,
                            r.name()
                        ))
                        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
                        None => PgError::error(format!("column \"{name}\" does not exist"))
                            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
                    });
                }
                // Check for duplicates.
                if attnums.iter().any(|&a| a == attnum) {
                    return Err(PgError::error(format!(
                        "column \"{name}\" specified more than once"
                    ))
                    .with_sqlstate(ERRCODE_DUPLICATE_COLUMN));
                }
                attnums.push(attnum);
            }
        }
    }

    Ok(attnums)
}

// ===========================================================================
// DoCopy (copy.c:61)
// ===========================================================================

/// `DoCopy(pstate, stmt, stmt_location, stmt_len, &processed)` (copy.c:61):
/// the COPY utility entry. Returns the number of rows processed.
pub fn DoCopy<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &Node<'mcx>,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<u64> {
    let Some(stmt) = stmt.as_copystmt() else {
        return Err(PgError::error("DoCopy: not a CopyStmt").with_sqlstate(ERRCODE_SYNTAX_ERROR));
    };

    let is_from = stmt.is_from;
    let pipe = stmt.filename.is_none();

    // Disallow COPY to/from file or program except to privileged roles.
    if !pipe {
        let userid = backend_utils_init_miscinit::GetUserId();
        if stmt.is_program {
            if !backend_utils_adt_acl::role_membership::has_privs_of_role(userid, ROLE_PG_EXECUTE_SERVER_PROGRAM)? {
                return Err(PgError::error(
                    "permission denied to COPY to or from an external program",
                )
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .with_detail(
                    "Only roles with privileges of the \"pg_execute_server_program\" role may COPY to or from an external program.",
                )
                .with_hint(
                    "Anyone can COPY to stdout or from stdin. psql's \\copy command also works for anyone.",
                ));
            }
        } else {
            if is_from && !backend_utils_adt_acl::role_membership::has_privs_of_role(userid, ROLE_PG_READ_SERVER_FILES)? {
                return Err(PgError::error("permission denied to COPY from a file")
                    .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .with_detail(
                        "Only roles with privileges of the \"pg_read_server_files\" role may COPY from a file.",
                    )
                    .with_hint(
                        "Anyone can COPY to stdout or from stdin. psql's \\copy command also works for anyone.",
                    ));
            }
            if !is_from && !backend_utils_adt_acl::role_membership::has_privs_of_role(userid, ROLE_PG_WRITE_SERVER_FILES)? {
                return Err(PgError::error("permission denied to COPY to a file")
                    .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .with_detail(
                        "Only roles with privileges of the \"pg_write_server_files\" role may COPY to a file.",
                    )
                    .with_hint(
                        "Anyone can COPY to stdout or from stdin. psql's \\copy command also works for anyone.",
                    ));
            }
        }
    }

    let mut rel: Option<types_rel::Relation<'mcx>> = None;
    let mut relid: Oid = 0; // InvalidOid
    let mut query: Option<RawStmt<'mcx>> = None;
    let mut where_clause: PgVec<'mcx, Expr> = PgVec::new_in(mcx);

    if let Some(relation_node) = stmt.relation.as_deref() {
        // stmt->relation is a RangeVar node.
        let Some(rv) = relation_node.as_rangevar() else {
            return Err(PgError::error("COPY: relation is not a RangeVar")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR));
        };
        let lockmode = if is_from { ROW_EXCLUSIVE_LOCK } else { ACCESS_SHARE_LOCK };

        debug_assert!(stmt.query.is_none());

        // Open and lock the relation.
        let access_rv = to_access_range_var(rv);
        let opened = backend_access_table_table::table_openrv(mcx, &access_rv, lockmode)?;
        relid = opened.rd_id;
        let local_temp = relation_is_local_temp(&opened);

        // addRangeTableEntryForRelation — adds an RTE + perminfo to pstate.
        let nsitem = backend_parser_relation::addRangeTableEntryForRelation(
            mcx,
            pstate,
            &opened,
            lockmode,
            None,
            false,
            false,
        )?;
        // perminfo = nsitem->p_perminfo; the just-added perminfo is the last.
        let perminfo_idx = pstate.p_rteperminfos.len() - 1;
        pstate.p_rteperminfos[perminfo_idx].requiredPerms =
            if is_from { ACL_INSERT } else { ACL_SELECT };

        if stmt.where_clause.is_some() {
            // COPY ... FROM ... WHERE — analyze the qual. (COPY FROM only; the
            // option-validation layer rejects WHERE for COPY TO.)
            where_clause = analyze_copy_where(mcx, pstate, nsitem, &opened, stmt)?;
        } else {
            // nsitem is only consumed (into the query namespace) on the WHERE
            // path; drop it otherwise (the RTE/perminfo are already in pstate).
            let _ = nsitem;
        }

        // attnums = CopyGetAttnums(tupDesc, rel, stmt->attlist)
        let tup_desc: TupleDesc<'mcx> = Some(opened.rd_att_clone_in(mcx)?);
        let attlist: Option<&[NodePtr<'mcx>]> =
            if stmt.attlist.is_empty() { None } else { Some(&stmt.attlist[..]) };
        let attnums = CopyGetAttnums(mcx, &tup_desc, Some(&opened), attlist)?;

        for attno in attnums.iter().copied() {
            let bms_idx = attno as i32 - FirstLowInvalidHeapAttributeNumber as i32;
            let pi = &mut pstate.p_rteperminfos[perminfo_idx];
            if is_from {
                pi.insertedCols = Some(backend_nodes_core::bitmapset::bms_add_member(
                    mcx,
                    pi.insertedCols.take(),
                    bms_idx,
                )?);
            } else {
                pi.selectedCols = Some(backend_nodes_core::bitmapset::bms_add_member(
                    mcx,
                    pi.selectedCols.take(),
                    bms_idx,
                )?);
            }
        }

        // ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), true)
        let perminfo_snapshot = pstate.p_rteperminfos[perminfo_idx].clone_in(mcx)?;
        backend_executor_execMain::exec_check_permissions(
            core::slice::from_ref(&perminfo_snapshot),
            true,
        )?;

        // Row-security check.
        let rls = backend_utils_misc_more::check_enable_rls(mcx, relid, 0, false)?;
        if rls == CheckEnableRlsResult::RlsEnabled {
            if is_from {
                return Err(PgError::error("COPY FROM not supported with row-level security")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_hint("Use INSERT statements instead."));
            }

            // Build the equivalent COPY (SELECT ... FROM ONLY rel) TO query and
            // hand it to the query-based COPY path.
            let raw = build_rls_select_query(mcx, &opened, stmt, stmt_location, stmt_len)?;
            query = Some(raw);

            // Close the relation but keep the lock.
            drop(opened);
            rel = None;
        } else {
            rel = Some(opened);
            let _ = local_temp; // consumed below only on the FROM read-only path
        }
    } else {
        // COPY (query) TO — stmt->query is the raw SELECT/INSERT/.../DELETE.
        debug_assert!(stmt.query.is_some());
        let q = stmt.query.as_ref().unwrap();
        query = Some(RawStmt {
            stmt: mcx::alloc_in(mcx, q.as_ref().clone_in(mcx)?)?,
            stmt_location,
            stmt_len,
        });
        relid = 0;
        rel = None;
    }

    let processed;
    if is_from {
        // COPY FROM file/stdin into the table.
        let rel = rel.as_ref().expect("COPY FROM requires an open relation");

        // check read-only transaction and parallel mode
        if backend_access_transam_xact_seams::xact_read_only::call()
            && !relation_is_local_temp(rel)
        {
            backend_access_transam_xact_seams::prevent_command_if_read_only::call("COPY FROM")?;
        }

        // BeginCopyFrom / CopyFrom / EndCopyFrom — copyfrom.c (Leg C), not yet
        // ported; this leg panics loudly until that owner lands.
        processed = copy_from_driver(
            mcx,
            pstate,
            rel,
            where_clause,
            stmt,
        )?;
    } else {
        // COPY TO file/stdout from the table or query.
        let raw_query = query.as_ref();
        let mut cstate = backend_commands_copyto::BeginCopyTo(
            mcx,
            Some(pstate),
            rel.take(),
            raw_query,
            relid,
            stmt.filename.as_deref(),
            stmt.is_program,
            None,
            if stmt.attlist.is_empty() { None } else { Some(&stmt.attlist[..]) },
            if let Some(o) = options_slice(stmt) { Some(o) } else { None },
        )?;
        processed = backend_commands_copyto::DoCopyTo(&mut cstate)?;
        backend_commands_copyto::EndCopyTo(cstate)?;
    }

    // The relation, if still open, is dropped here (table_close(rel, NoLock));
    // RAII releases the buffer pin while keeping the lock to xact end.
    drop(rel);

    Ok(processed)
}

/// `stmt->options` as a slice (`None` ⇒ NIL).
fn options_slice<'mcx, 'a>(stmt: &'a CopyStmt<'mcx>) -> Option<&'a [NodePtr<'mcx>]> {
    if stmt.options.is_empty() {
        None
    } else {
        Some(&stmt.options[..])
    }
}

/// `RelationIsLocalTemp(rel)` — a temp rel of this session.
fn relation_is_local_temp(rel: &types_rel::Relation<'_>) -> bool {
    rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        && rel.rd_backend == backend_storage_lmgr_proc_seams::my_proc_number::call()
}

/// COPY ... FROM ... WHERE: transform / coerce / collate / fold the qual.
/// (copy.c:134-191). Only reached on COPY FROM. Returns the preprocessed qual
/// as the implicitly-ANDed list of `Expr` (the C `make_ands_implicit` result,
/// the `List *` stored on `cstate->whereClause`); an empty list ⇒ no qual.
fn analyze_copy_where<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    nsitem: types_nodes::parsestmt::ParseNamespaceItem<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    stmt: &CopyStmt<'mcx>,
) -> PgResult<PgVec<'mcx, Expr<'mcx>>> {
    use types_nodes::parsestmt::ParseExprKind::EXPR_KIND_COPY_WHERE;

    // add nsitem to query namespace
    backend_parser_relation::addNSItemToQuery(mcx, pstate, nsitem, false, true, true)?;

    // Transform the raw expression tree.
    //   whereClause = transformExpr(pstate, stmt->whereClause, EXPR_KIND_COPY_WHERE);
    let raw = match stmt.where_clause.as_deref() {
        Some(n) => Some(n.clone_in(mcx)?),
        None => None,
    };
    let transformed =
        backend_parser_parse_expr_seams::transformExpr::call(pstate, raw, EXPR_KIND_COPY_WHERE)?;
    let where_clause = transformed
        .expect("COPY WHERE: transformExpr of a non-NULL raw clause yields a non-NULL Expr");

    // Make sure it yields a boolean result.
    //   whereClause = coerce_to_boolean(pstate, whereClause, "WHERE");
    let where_clause =
        backend_parser_coerce::coerce_to_boolean(mcx, Some(pstate), where_clause, "WHERE")?;
    // Bring the parser-arena `'static` result into `mcx` for the in-place
    // collation pass and the `eval_const_expressions(mcx, ..)` fold below
    // (`Expr` is invariant over its lifetime).
    let mut where_clause: Expr<'mcx> = where_clause.clone_in(mcx)?;

    // We have to fix its collations too.
    //   assign_expr_collations(pstate, whereClause);
    backend_parser_parse_collate::assign_expr_collations(Some(pstate), &mut where_clause)?;

    // Examine all the columns in the WHERE clause expression.  When the
    // whole-row reference is present, examine all the columns of the table.
    //   pull_varattnos(whereClause, 1, &expr_attrs);
    let mut expr_attrs =
        backend_optimizer_util_var_seams::pull_varattnos::call(mcx, &where_clause, 1)?;
    let first_low = FirstLowInvalidHeapAttributeNumber as i32;
    let whole_row_member = 0 - first_low;
    if backend_nodes_core::bitmapset::bms_is_member(whole_row_member, expr_attrs.as_deref()) {
        // expand to all real columns, then drop the whole-row marker
        expr_attrs = backend_nodes_core::bitmapset::bms_add_range(
            mcx,
            expr_attrs,
            1 - first_low,
            rel.rd_att.natts as i32 - first_low,
        )?;
        expr_attrs =
            backend_nodes_core::bitmapset::bms_del_member(expr_attrs, whole_row_member);
    }

    let mut i = -1;
    loop {
        i = backend_nodes_core::bitmapset::bms_next_member(expr_attrs.as_deref(), i);
        if i < 0 {
            break;
        }
        let attno = (i + first_low) as AttrNumber;
        debug_assert!(attno != 0);

        // Prohibit generated columns in the WHERE clause. Stored generated
        // columns are not yet computed when the filtering happens; virtual
        // generated columns are kept consistent with the stored variant.
        if rel.rd_att.attrs[(attno - 1) as usize].attgenerated != 0 {
            let colname =
                backend_utils_cache_lsyscache::attribute::get_attname(mcx, rel.rd_id, attno, false)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
            return Err(PgError::error(
                "generated columns are not supported in COPY FROM WHERE conditions",
            )
            .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE)
            .with_detail(format!("Column \"{colname}\" is a generated column.")));
        }
    }

    // whereClause = eval_const_expressions(NULL, whereClause);
    let where_clause = backend_optimizer_util_clauses::eval_const_expressions(mcx, where_clause)?;

    // whereClause = (Node *) canonicalize_qual((Expr *) whereClause, false);
    let canon =
        backend_optimizer_prep_prepqual::canonicalize_qual(mcx, Some(where_clause), false)?;

    // whereClause = (Node *) make_ands_implicit((Expr *) whereClause);
    let implicit = backend_nodes_core::makefuncs::make_ands_implicit(canon);
    let mut out: PgVec<'mcx, Expr<'mcx>> = mcx::vec_with_capacity_in(mcx, implicit.len())?;
    for e in implicit {
        out.push(e);
    }
    Ok(out)
}

/// The COPY FROM driver leg (copyfrom.c): `BeginCopyFrom` / `CopyFrom` /
/// `EndCopyFrom` (commands/copyfrom.c, owned by `backend-commands-copyfrom`).
fn copy_from_driver<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    where_clause: PgVec<'mcx, Expr<'mcx>>,
    stmt: &CopyStmt<'mcx>,
) -> PgResult<u64> {
    // ProcessCopyOptions(pstate, &opts, true /* is_from */, options).
    let fmt = ProcessCopyOptions(
        mcx,
        Some(pstate),
        /* is_from = */ true,
        options_slice(stmt),
    )?;

    // Project the parse-relevant subset of CopyFormatOptions onto the parser's
    // CopyParseOptions.
    let opts = types_copy::CopyParseOptions {
        binary: fmt.binary,
        csv_mode: fmt.csv_mode,
        header_line: fmt.header_line,
        null_print: fmt.null_print.as_str().to_string(),
        null_print_len: fmt.null_print_len,
        default_print: fmt.default_print.as_ref().map(|s| s.as_str().to_string()),
        default_print_len: fmt.default_print_len,
        delim: fmt.delim,
        quote: fmt.quote,
        escape: fmt.escape,
        on_error: fmt.on_error,
        log_verbosity: fmt.log_verbosity,
    };

    // attnumlist = CopyGetAttnums(tupDesc, rel, attlist).
    let tup_desc: TupleDesc<'mcx> = Some(rel.rd_att_clone_in(mcx)?);
    let attlist: Option<&[NodePtr<'mcx>]> =
        if stmt.attlist.is_empty() { None } else { Some(&stmt.attlist[..]) };
    let attnumlist = CopyGetAttnums(mcx, &tup_desc, Some(rel), attlist)?;

    // range_table / rteperminfos: a fresh owned copy of pstate's (the driver's
    // EState takes ownership via ExecInitRangeTable).
    let mut range_table: PgVec<'mcx, types_nodes::RangeTblEntry<'mcx>> =
        PgVec::new_in(mcx);
    for rte in pstate.p_rtable.iter() {
        range_table.push(rte.clone_in(mcx)?);
    }
    let mut rteperminfos: PgVec<'mcx, types_nodes::RTEPermissionInfo<'mcx>> =
        PgVec::new_in(mcx);
    for pi in pstate.p_rteperminfos.iter() {
        rteperminfos.push(pi.clone_in(mcx)?);
    }

    let file_encoding = fmt.file_encoding;

    let mut state = backend_commands_copyfrom::BeginCopyFrom(
        mcx,
        rel.alias(),
        opts,
        file_encoding,
        attnumlist,
        range_table,
        rteperminfos,
        stmt.filename.as_deref(),
        stmt.is_program,
        // DoCopy (the SQL `COPY ... FROM` path) never supplies a programmatic
        // data source — that is the SPI/extension `BeginCopyFrom` entry only.
        None,
        where_clause,
    )?;
    let processed = backend_commands_copyfrom::CopyFrom(mcx, &mut state)?;
    backend_commands_copyfrom::EndCopyFrom(state)?;
    Ok(processed)
}

/// Build the `SELECT ... FROM ONLY rel` raw query the RLS path runs as a
/// query-based COPY (copy.c:221-317).
fn build_rls_select_query<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    stmt: &CopyStmt<'mcx>,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<RawStmt<'mcx>> {
    // targetList: '*' if no attlist, else one ColumnRef per column.
    let mut target_list: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    if stmt.attlist.is_empty() {
        let cr = ColumnRef {
            fields: {
                let mut v = PgVec::new_in(mcx);
                v.push(mcx::alloc_in(mcx, Node::mk_a_star(mcx, types_nodes::rawnodes::A_Star)?)?);
                v
            },
            location: -1,
        };
        target_list.push(make_res_target(mcx, Node::mk_column_ref(mcx, cr)?)?);
    } else {
        for col in stmt.attlist.iter() {
            let cr = ColumnRef {
                fields: {
                    let mut v = PgVec::new_in(mcx);
                    v.push(mcx::alloc_in(mcx, col.as_ref().clone_in(mcx)?)?);
                    v
                },
                location: -1,
            };
            target_list.push(make_res_target(mcx, Node::mk_column_ref(mcx, cr)?)?);
        }
    }

    // fromClause: makeRangeVar(get_namespace_name(rel ns), relname, -1); inh=false
    let nspname = backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name(
        mcx,
        rel.rd_rel.relnamespace,
    )?;
    let from = types_nodes::rawnodes::RangeVar {
        catalogname: None,
        schemaname: match nspname {
            Some(s) => Some(s.clone_in(mcx)?),
            None => None,
        },
        relname: Some(PgString::from_str_in(rel.name(), mcx)?),
        inh: false, // apply ONLY
        relpersistence: types_tuple::access::RELPERSISTENCE_PERMANENT as i8,
        alias: None,
        location: -1,
    };

    let mut from_clause: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    from_clause.push(mcx::alloc_in(mcx, Node::mk_range_var(mcx, from)?)?);

    let select = empty_select_stmt(mcx, target_list, from_clause)?;

    Ok(RawStmt {
        stmt: mcx::alloc_in(mcx, Node::mk_select_stmt(mcx, select)?)?,
        stmt_location,
        stmt_len,
    })
}

/// `makeNode(ResTarget)` with `val` and the rest NULL/NIL (copy.c).
fn make_res_target<'mcx>(mcx: Mcx<'mcx>, val: Node<'mcx>) -> PgResult<NodePtr<'mcx>> {
    let rt = ResTarget {
        name: None,
        indirection: PgVec::new_in(mcx),
        val: Some(mcx::alloc_in(mcx, val)?),
        location: -1,
    };
    mcx::alloc_in(mcx, Node::mk_res_target(mcx, rt)?)
}

/// A leaf `SELECT targetList FROM fromClause` (all other fields NIL/default).
fn empty_select_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: PgVec<'mcx, NodePtr<'mcx>>,
    from_clause: PgVec<'mcx, NodePtr<'mcx>>,
) -> PgResult<SelectStmt<'mcx>> {
    Ok(SelectStmt {
        distinctClause: PgVec::new_in(mcx),
        intoClause: None,
        targetList: target_list,
        fromClause: from_clause,
        whereClause: None,
        groupClause: PgVec::new_in(mcx),
        groupDistinct: false,
        havingClause: None,
        windowClause: PgVec::new_in(mcx),
        valuesLists: PgVec::new_in(mcx),
        sortClause: PgVec::new_in(mcx),
        limitOffset: None,
        limitCount: None,
        limitOption: LimitOption::LIMIT_OPTION_COUNT,
        lockingClause: PgVec::new_in(mcx),
        withClause: None,
        op: SetOperation::SETOP_NONE,
        all: false,
        larg: None,
        rarg: None,
    })
}

// ===========================================================================
// in-crate helpers
// ===========================================================================

const ACCESS_SHARE_LOCK: i32 = 1;
const ROW_EXCLUSIVE_LOCK: i32 = 3;

/// `pg_strcasecmp(a, b) == 0` — ASCII case-insensitive equality.
fn pg_strcasecmp(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes().zip(b.bytes()).all(|(x, y)| x.eq_ignore_ascii_case(&y))
}

fn binary_mode_error(name: &str) -> PgError {
    PgError::error(format!("cannot specify {name} in BINARY mode"))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
}

fn binary_mode_feature_error(name: &str) -> PgError {
    PgError::error(format!("cannot specify {name} in BINARY mode"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

fn requires_csv_error(name: &str) -> PgError {
    PgError::error(format!("COPY {name} requires CSV mode"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

fn cannot_be_used_with(
    option: &str,
    direction: &str,
    code: types_error::SqlState,
) -> PgError {
    PgError::error(format!("COPY {option} cannot be used with {direction}")).with_sqlstate(code)
}

/// Bridge the owned grammar `RangeVar` node into the lifetime-free
/// `access::RangeVar` the table-open machinery consumes.
fn to_access_range_var(rv: &types_nodes::rawnodes::RangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.to_string()),
        relname: rv.relname.as_ref().map(|s| s.to_string()).unwrap_or_default(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

fn force_list_error(dn: &str, pstate: Option<&ParseState<'_>>, defel: &DefElem) -> PgError {
    PgError::error(format!(
        "argument to option \"{dn}\" must be a list of column names"
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
    .with_cursor_position(errpos(pstate, defel.location))
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the three inward seams this crate owns: the two shared COPY-option
/// seams and the utility dispatcher's `do_copy` entry.
pub fn init_seams() {
    backend_commands_copy_seams::process_copy_options::set(seam_process_copy_options);
    backend_commands_copy_seams::copy_get_attnums::set(seam_copy_get_attnums);
    backend_tcop_utility_out_seams::do_copy::set(seam_do_copy);
}

fn seam_process_copy_options<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&ParseState<'mcx>>,
    is_from: bool,
    options: Option<&[NodePtr<'mcx>]>,
) -> PgResult<CopyFormatOptions<'mcx>> {
    ProcessCopyOptions(mcx, pstate, is_from, options)
}

fn seam_copy_get_attnums<'mcx>(
    mcx: Mcx<'mcx>,
    tup_desc: &TupleDesc<'mcx>,
    rel: Option<&types_rel::Relation<'mcx>>,
    attnamelist: Option<&[NodePtr<'mcx>]>,
) -> PgResult<PgVec<'mcx, AttrNumber>> {
    CopyGetAttnums(mcx, tup_desc, rel, attnamelist)
}

fn seam_do_copy<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &Node<'mcx>,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<u64> {
    DoCopy(mcx, pstate, stmt, stmt_location, stmt_len)
}
