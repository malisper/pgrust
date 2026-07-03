// copy.c/copyto.c/copyfrom.c/copyfromparse.c — text + CSV formats, file and
// wire STDIN/STDOUT variants; column defaults, the DEFAULT marker and
// FROM ... WHERE live. Loud (named): binary format, COPY (query), PROGRAM,
// ON_ERROR ignore, FREEZE, HEADER match, volatile defaults/WHERE, generated
// columns, RLS rewrite. Option parsing (ProcessCopyOptions) is full-parity.
#![allow(non_snake_case)]

use mcx::{vec_from_elem_in, Mcx, PgVec};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
};
use types_nodes::parsenodes::CopyStmt;
use types_nodes::{Node, NodeList};
use types_rel::Relation;
use types_tuple::TupleDescData;

mod from;
mod fromparse;
mod to;
#[cfg(test)]
mod tests;

pub use from::{BeginCopyFrom, CopyFrom, EndCopyFrom};
pub use to::{BeginCopyTo, DoCopyTo, EndCopyTo};
#[doc(hidden)]
pub use fromparse::bench_internals;
#[doc(hidden)]
pub use to::copy_attribute_out_text;

const ROLE_PG_READ_SERVER_FILES: Oid = 4569;
const ROLE_PG_WRITE_SERVER_FILES: Oid = 4570;

const ACL_INSERT: u64 = 1 << 0;
const ACL_SELECT: u64 = 1 << 1;
const ACLCHECK_OK: i32 = 0;

const RELKIND_RELATION: u8 = b'r';

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: COPY {what}")
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyOnErrorChoice {
    #[default]
    Stop,
    Ignore,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum CopyLogVerbosityChoice {
    Silent,
    #[default]
    Default,
    Verbose,
}

pub struct CopyFormatOptions<'s> {
    pub file_encoding: i32,
    pub binary: bool,
    pub csv_mode: bool,
    pub freeze: bool,
    pub delim: u8,
    pub quote: u8,
    pub escape: u8,
    pub null_print: &'s str,
    pub default_print: Option<&'s str>,
    pub header_line: bool,
    pub force_quote: Option<&'s NodeList<'s>>,
    pub force_quote_all: bool,
    pub force_notnull: Option<&'s NodeList<'s>>,
    pub force_notnull_all: bool,
    pub force_null: Option<&'s NodeList<'s>>,
    pub force_null_all: bool,
    pub on_error: CopyOnErrorChoice,
    pub log_verbosity: CopyLogVerbosityChoice,
    pub reject_limit: i64,
}

fn errpos(src: Option<&str>, location: types_core::ParseLoc) -> i32 {
    parser_small1::parser_errposition_source(
        src.map(str::as_bytes),
        location,
        mbutils::GetDatabaseEncoding(),
    )
}

/// `DoCopy` (copy.c). Returns rows processed.
pub fn DoCopy<'mcx>(mcx: Mcx<'mcx>, stmt: &CopyStmt<'mcx>, source_text: &str) -> PgResult<u64> {
    let is_from = stmt.is_from;
    if stmt.is_program {
        unported("TO/FROM PROGRAM (OpenPipeStream lane)");
    }

    let userid = miscinit_seams::get_user_id::call();
    if stmt.filename.is_some() {
        let (role, denied) = if is_from {
            (ROLE_PG_READ_SERVER_FILES, from_file_denied as fn() -> Box<PgError>)
        } else {
            (ROLE_PG_WRITE_SERVER_FILES, to_file_denied as fn() -> Box<PgError>)
        };
        if !acl_seams::has_privs_of_role::call(userid, role)? {
            return Err(denied());
        }
    }

    let Some(rv_node) = stmt.relation else {
        // C divergence: C analyzes the query before ProcessCopyOptions (inside
        // BeginCopyTo); options are validated here first so option errors keep
        // C's text while the executor lane stays loud.
        ProcessCopyOptions(false, &stmt.options, Some(source_text))?;
        unported("(query) TO (pg_analyze_and_rewrite + executor lane)");
    };
    let rv = rv_node.as_range_var().expect("CopyStmt.relation is RangeVar");
    let rv = rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname: rv.relname.expect("RangeVar.relname"),
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    };

    let lockmode = if is_from {
        types_rel::lock::RowExclusiveLock
    } else {
        types_rel::lock::AccessShareLock
    };
    let rel = table::table_openrv(mcx, &rv, lockmode)?;

    // ExecCheckPermissions, relation-level arm. Column-level GRANT is
    // unported, so the column fallback reduces to the plain denial error.
    let required = if is_from { ACL_INSERT } else { ACL_SELECT };
    let r = aclchk_seams::object_aclcheck::call(
        types_core::catalog::RELATION_RELATION_ID,
        rel.rd_id,
        userid,
        required,
    )?;
    if r != ACLCHECK_OK {
        // OBJECT_TABLE discriminant (parsenodes.h ObjectType).
        aclchk_seams::aclcheck_error::call(r, 41, rv.relname)?;
    }
    if rel.rd_rel.relrowsecurity {
        unported("with row-level security (query-rewrite arm)");
    }

    let mut where_clause = NodeList::nil();
    if let Some(wc) = stmt.whereClause {
        let mut pstate = parser_small1::make_parsestate(mcx, None);
        {
            let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
            mcx::vec_append_bytes(&mut v, source_text.as_bytes())
                .map_err(|_| mcx.oom(source_text.len()))?;
            pstate.p_sourcetext = Some(v.leak());
        }
        let nsitem =
            parse_relation::addRangeTableEntryForRelation(mcx, &mut pstate, &rel, lockmode, None, false, false)?;
        parse_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;
        let qual = parse_clause::transformWhereClause(
            mcx,
            &mut pstate,
            Some(wc),
            parser_small1::ParseExprKind::EXPR_KIND_COPY_WHERE,
            "WHERE",
        )?
        .expect("clause in, clause out");
        parse_collate::assign_expr_collations(mcx, &pstate, qual)?;
        // C divergence: the pull_varattnos generated-column screen is elided
        // (generated-column relations are loud in BeginCopyFrom).
        let qual = clauses::eval_const_expressions(mcx, qual)?;
        let qual = planner::prepqual::canonicalize_qual(mcx, qual, false)?;
        where_clause = clauses::make_ands_implicit(mcx, Some(qual))?;
        parser_small1::free_parsestate(pstate)?;
    }

    let processed = if is_from {
        if xact::XactReadOnly() && !rel.rd_islocaltemp {
            xact::PreventCommandIfReadOnly("COPY FROM")?;
        }
        let mut cstate = BeginCopyFrom(
            mcx,
            &rel,
            where_clause,
            stmt.filename,
            &stmt.attlist,
            &stmt.options,
            Some(source_text),
        )?;
        let processed = CopyFrom(mcx, &mut cstate, &rel)?;
        EndCopyFrom(cstate)?;
        processed
    } else {
        let mut cstate = BeginCopyTo(
            mcx,
            &rel,
            stmt.filename,
            &stmt.attlist,
            &stmt.options,
            Some(source_text),
        )?;
        let processed = DoCopyTo(mcx, &mut cstate, &rel)?;
        EndCopyTo(cstate)?;
        processed
    };

    table::table_close(rel, types_rel::lock::NoLock)?;
    Ok(processed)
}

fn def_string<'a>(d: &types_nodes::parsenodes::DefElem<'a>) -> PgResult<&'a str> {
    match d.arg {
        Some(n) => match n.as_string() {
            Some(s) => Ok(s.sval),
            None => panic!(
                "defGetString (define.c): non-String arg arm not ported for option {:?}",
                d.defname
            ),
        },
        None => Err(Box::new(
            PgError::error(format!("{} requires a parameter", d.defname.unwrap_or("")))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )),
    }
}

fn def_list_or_star<'s>(
    d: &types_nodes::parsenodes::DefElem<'s>,
    src: Option<&str>,
) -> PgResult<(Option<&'s NodeList<'s>>, bool)> {
    if let Some(arg) = d.arg {
        if arg.as_a_star().is_some() {
            return Ok((None, true));
        }
        if let Some(l) = arg.as_list() {
            return Ok((Some(l), false));
        }
    }
    Err(Box::new(
        PgError::error(format!(
            "argument to option \"{}\" must be a list of column names",
            d.defname.unwrap_or("")
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .with_cursor_position(errpos(src, d.location)),
    ))
}

// defGetBoolean (define.c), the arms COPY's gram can produce.
fn def_boolean(d: &types_nodes::parsenodes::DefElem<'_>) -> PgResult<bool> {
    let Some(arg) = d.arg else { return Ok(true) };
    if let Some(i) = arg.as_integer() {
        match i.ival {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {}
        }
    } else {
        let sval = if let Some(b) = arg.as_boolean() {
            if b.boolval {
                "true"
            } else {
                "false"
            }
        } else {
            def_string(d)?
        };
        if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
            return Ok(true);
        }
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(false);
        }
    }
    Err(Box::new(
        PgError::error(format!("{} requires a Boolean value", d.defname.unwrap_or("")))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    ))
}

// defGetCopyHeaderChoice (copy.c); COPY_HEADER_MATCH is loud.
fn def_header_choice(d: &types_nodes::parsenodes::DefElem<'_>, is_from: bool) -> PgResult<bool> {
    let Some(arg) = d.arg else { return Ok(true) };
    if let Some(i) = arg.as_integer() {
        match i.ival {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {}
        }
    } else {
        let sval = if let Some(b) = arg.as_boolean() {
            if b.boolval {
                "true"
            } else {
                "false"
            }
        } else if let Some(s) = arg.as_string() {
            s.sval
        } else {
            ""
        };
        if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
            return Ok(true);
        }
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(false);
        }
        if sval.eq_ignore_ascii_case("match") {
            if !is_from {
                return Err(Box::new(
                    PgError::error(format!("cannot use \"{sval}\" with HEADER in COPY TO"))
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            unported("HEADER match");
        }
    }
    Err(Box::new(
        PgError::error(format!(
            "{} requires a Boolean value or \"match\"",
            d.defname.unwrap_or("")
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    ))
}

// defGetCopyOnErrorChoice (copy.c).
fn def_on_error_choice(
    d: &types_nodes::parsenodes::DefElem<'_>,
    is_from: bool,
    src: Option<&str>,
) -> PgResult<CopyOnErrorChoice> {
    let sval = def_string(d)?;
    if !is_from {
        return Err(Box::new(
            PgError::error("COPY ON_ERROR cannot be used with COPY TO")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_cursor_position(errpos(src, d.location)),
        ));
    }
    if sval.eq_ignore_ascii_case("stop") {
        return Ok(CopyOnErrorChoice::Stop);
    }
    if sval.eq_ignore_ascii_case("ignore") {
        return Ok(CopyOnErrorChoice::Ignore);
    }
    Err(Box::new(
        PgError::error(format!("COPY ON_ERROR \"{sval}\" not recognized"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_cursor_position(errpos(src, d.location)),
    ))
}

// defGetCopyLogVerbosityChoice (copy.c).
fn def_log_verbosity_choice(
    d: &types_nodes::parsenodes::DefElem<'_>,
    src: Option<&str>,
) -> PgResult<CopyLogVerbosityChoice> {
    let sval = def_string(d)?;
    if sval.eq_ignore_ascii_case("silent") {
        return Ok(CopyLogVerbosityChoice::Silent);
    }
    if sval.eq_ignore_ascii_case("default") {
        return Ok(CopyLogVerbosityChoice::Default);
    }
    if sval.eq_ignore_ascii_case("verbose") {
        return Ok(CopyLogVerbosityChoice::Verbose);
    }
    Err(Box::new(
        PgError::error(format!("COPY LOG_VERBOSITY \"{sval}\" not recognized"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_cursor_position(errpos(src, d.location)),
    ))
}

// defGetCopyRejectLimitOption (copy.c); the Sconst (file_fdw) arm is loud.
fn def_reject_limit(d: &types_nodes::parsenodes::DefElem<'_>) -> PgResult<i64> {
    let reject_limit = match d.arg {
        None => {
            return Err(Box::new(
                PgError::error(format!(
                    "{} requires a numeric value",
                    d.defname.unwrap_or("")
                ))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ))
        }
        Some(n) => match n.as_integer() {
            Some(i) => i.ival as i64,
            None => panic!(
                "defGetCopyRejectLimitOption (copy.c): non-Integer REJECT_LIMIT arm \
                 (pg_strtoint64/defGetInt64) not ported"
            ),
        },
    };
    if reject_limit <= 0 {
        return Err(Box::new(
            PgError::error(format!("REJECT_LIMIT ({reject_limit}) must be greater than zero"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(reject_limit)
}

#[cold]
#[inline(never)]
fn requires_csv(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("COPY {name} requires CSV mode"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[cold]
#[inline(never)]
fn cannot_in_binary(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot specify {name} in BINARY mode"))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

/// `ProcessCopyOptions` (copy.c). `src` is the statement source text for
/// error cursors (C's pstate->p_sourcetext).
pub fn ProcessCopyOptions<'s>(
    is_from: bool,
    options: &NodeList<'s>,
    src: Option<&str>,
) -> PgResult<CopyFormatOptions<'s>> {
    let mut opts = CopyFormatOptions {
        file_encoding: -1,
        binary: false,
        csv_mode: false,
        freeze: false,
        delim: 0,
        quote: 0,
        escape: 0,
        null_print: "",
        default_print: None,
        header_line: false,
        force_quote: None,
        force_quote_all: false,
        force_notnull: None,
        force_notnull_all: false,
        force_null: None,
        force_null_all: false,
        on_error: CopyOnErrorChoice::Stop,
        log_verbosity: CopyLogVerbosityChoice::Default,
        reject_limit: 0,
    };
    let mut format_specified = false;
    let mut freeze_specified = false;
    let mut header_specified = false;
    let mut on_error_specified = false;
    let mut log_verbosity_specified = false;
    let mut reject_limit_specified = false;
    let mut delim: Option<&str> = None;
    let mut null_print: Option<&str> = None;
    let mut quote: Option<&str> = None;
    let mut escape: Option<&str> = None;

    for option in options.iter() {
        let d = option.as_def_elem().expect("COPY options: DefElem list");
        let name = d.defname.unwrap_or("");
        match name {
            "format" => {
                if format_specified {
                    return Err(conflicting_option(src, d.location));
                }
                format_specified = true;
                match def_string(d)? {
                    "text" => {}
                    "csv" => opts.csv_mode = true,
                    "binary" => opts.binary = true,
                    fmt => {
                        return Err(Box::new(
                            PgError::error(format!("COPY format \"{fmt}\" not recognized"))
                                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                                .with_cursor_position(errpos(src, d.location)),
                        ))
                    }
                }
            }
            "freeze" => {
                if freeze_specified {
                    return Err(conflicting_option(src, d.location));
                }
                freeze_specified = true;
                opts.freeze = def_boolean(d)?;
            }
            "delimiter" => {
                if delim.is_some() {
                    return Err(conflicting_option(src, d.location));
                }
                delim = Some(def_string(d)?);
            }
            "null" => {
                if null_print.is_some() {
                    return Err(conflicting_option(src, d.location));
                }
                null_print = Some(def_string(d)?);
            }
            "default" => {
                if opts.default_print.is_some() {
                    return Err(conflicting_option(src, d.location));
                }
                opts.default_print = Some(def_string(d)?);
            }
            "header" => {
                if header_specified {
                    return Err(conflicting_option(src, d.location));
                }
                header_specified = true;
                opts.header_line = def_header_choice(d, is_from)?;
            }
            "quote" => {
                if quote.is_some() {
                    return Err(conflicting_option(src, d.location));
                }
                quote = Some(def_string(d)?);
            }
            "escape" => {
                if escape.is_some() {
                    return Err(conflicting_option(src, d.location));
                }
                escape = Some(def_string(d)?);
            }
            "force_quote" => {
                if opts.force_quote.is_some() || opts.force_quote_all {
                    return Err(conflicting_option(src, d.location));
                }
                (opts.force_quote, opts.force_quote_all) = def_list_or_star(d, src)?;
            }
            "force_not_null" => {
                if opts.force_notnull.is_some() || opts.force_notnull_all {
                    return Err(conflicting_option(src, d.location));
                }
                (opts.force_notnull, opts.force_notnull_all) = def_list_or_star(d, src)?;
            }
            "force_null" => {
                if opts.force_null.is_some() || opts.force_null_all {
                    return Err(conflicting_option(src, d.location));
                }
                (opts.force_null, opts.force_null_all) = def_list_or_star(d, src)?;
            }
            "convert_selectively" => unported("convert_selectively"),
            "encoding" => {
                if opts.file_encoding >= 0 {
                    return Err(conflicting_option(src, d.location));
                }
                opts.file_encoding = mbutils::pg_char_to_encoding(def_string(d)?);
                if opts.file_encoding < 0 {
                    return Err(Box::new(
                        PgError::error(format!(
                            "argument to option \"{name}\" must be a valid encoding name"
                        ))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                        .with_cursor_position(errpos(src, d.location)),
                    ));
                }
            }
            "on_error" => {
                if on_error_specified {
                    return Err(conflicting_option(src, d.location));
                }
                on_error_specified = true;
                opts.on_error = def_on_error_choice(d, is_from, src)?;
            }
            "log_verbosity" => {
                if log_verbosity_specified {
                    return Err(conflicting_option(src, d.location));
                }
                log_verbosity_specified = true;
                opts.log_verbosity = def_log_verbosity_choice(d, src)?;
            }
            "reject_limit" => {
                if reject_limit_specified {
                    return Err(conflicting_option(src, d.location));
                }
                reject_limit_specified = true;
                opts.reject_limit = def_reject_limit(d)?;
            }
            other => {
                return Err(Box::new(
                    PgError::error(format!("option \"{other}\" not recognized"))
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                        .with_cursor_position(errpos(src, d.location)),
                ))
            }
        }
    }

    if opts.binary && delim.is_some() {
        return Err(cannot_in_binary("DELIMITER"));
    }
    if opts.binary && null_print.is_some() {
        return Err(cannot_in_binary("NULL"));
    }
    if opts.binary && opts.default_print.is_some() {
        return Err(cannot_in_binary("DEFAULT"));
    }

    let delim = delim.unwrap_or(if opts.csv_mode { "," } else { "\t" });
    opts.null_print = null_print.unwrap_or(if opts.csv_mode { "" } else { "\\N" });
    let quote = if opts.csv_mode { Some(quote.unwrap_or("\"")) } else { quote };
    let escape = if opts.csv_mode { Some(escape.unwrap_or(quote.unwrap())) } else { escape };

    if delim.len() != 1 {
        return Err(Box::new(
            PgError::error("COPY delimiter must be a single one-byte character")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    opts.delim = delim.as_bytes()[0];
    if opts.delim == b'\r' || opts.delim == b'\n' {
        return Err(Box::new(
            PgError::error("COPY delimiter cannot be newline or carriage return")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if opts.null_print.contains('\r') || opts.null_print.contains('\n') {
        return Err(Box::new(
            PgError::error("COPY null representation cannot use newline or carriage return")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if let Some(default_print) = opts.default_print {
        if default_print.contains('\r') || default_print.contains('\n') {
            return Err(Box::new(
                PgError::error(
                    "COPY default representation cannot use newline or carriage return",
                )
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        }
    }
    if !opts.csv_mode && b"\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(&opts.delim) {
        return Err(Box::new(
            PgError::error(format!("COPY delimiter cannot be \"{}\"", opts.delim as char))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if opts.binary && opts.header_line {
        return Err(cannot_in_binary("HEADER"));
    }
    if !opts.csv_mode && quote.is_some() {
        return Err(requires_csv("QUOTE"));
    }
    if let Some(quote) = quote {
        if quote.len() != 1 {
            return Err(Box::new(
                PgError::error("COPY quote must be a single one-byte character")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        opts.quote = quote.as_bytes()[0];
    }
    if opts.csv_mode && opts.delim == opts.quote {
        return Err(Box::new(
            PgError::error("COPY delimiter and quote must be different")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if !opts.csv_mode && escape.is_some() {
        return Err(requires_csv("ESCAPE"));
    }
    if let Some(escape) = escape {
        if escape.len() != 1 {
            return Err(Box::new(
                PgError::error("COPY escape must be a single one-byte character")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        opts.escape = escape.as_bytes()[0];
    }
    if !opts.csv_mode && (opts.force_quote.is_some() || opts.force_quote_all) {
        return Err(requires_csv("FORCE_QUOTE"));
    }
    if (opts.force_quote.is_some() || opts.force_quote_all) && is_from {
        return Err(Box::new(
            PgError::error("COPY FORCE_QUOTE cannot be used with COPY FROM")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if !opts.csv_mode && (opts.force_notnull.is_some() || opts.force_notnull_all) {
        return Err(requires_csv("FORCE_NOT_NULL"));
    }
    if (opts.force_notnull.is_some() || opts.force_notnull_all) && !is_from {
        return Err(Box::new(
            PgError::error("COPY FORCE_NOT_NULL cannot be used with COPY TO")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if !opts.csv_mode && (opts.force_null.is_some() || opts.force_null_all) {
        return Err(requires_csv("FORCE_NULL"));
    }
    if (opts.force_null.is_some() || opts.force_null_all) && !is_from {
        return Err(Box::new(
            PgError::error("COPY FORCE_NULL cannot be used with COPY TO")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if opts.null_print.as_bytes().contains(&opts.delim) {
        return Err(Box::new(
            PgError::error("COPY delimiter character must not appear in the NULL specification")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if opts.csv_mode && opts.null_print.as_bytes().contains(&opts.quote) {
        return Err(Box::new(
            PgError::error("CSV quote character must not appear in the NULL specification")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if opts.freeze && !is_from {
        return Err(Box::new(
            PgError::error("COPY FREEZE cannot be used with COPY TO")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if let Some(default_print) = opts.default_print {
        if !is_from {
            return Err(Box::new(
                PgError::error("COPY DEFAULT cannot be used with COPY TO")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if default_print.as_bytes().contains(&opts.delim) {
            return Err(Box::new(
                PgError::error(
                    "COPY delimiter character must not appear in the DEFAULT specification",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if opts.csv_mode && default_print.as_bytes().contains(&opts.quote) {
            return Err(Box::new(
                PgError::error(
                    "CSV quote character must not appear in the DEFAULT specification",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if opts.null_print == default_print {
            return Err(Box::new(
                PgError::error(
                    "NULL specification and DEFAULT specification cannot be the same",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
    }
    if opts.binary && opts.on_error != CopyOnErrorChoice::Stop {
        return Err(Box::new(
            PgError::error("only ON_ERROR STOP is allowed in BINARY mode")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if opts.reject_limit != 0 && opts.on_error != CopyOnErrorChoice::Ignore {
        return Err(Box::new(
            PgError::error("COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(opts)
}

// force_quote/force_notnull/force_null -> per-physical-attr flags, with C's
// "not referenced by COPY" checks (BeginCopyTo/BeginCopyFrom).
fn force_flags<'mcx>(
    mcx: Mcx<'mcx>,
    tup_desc: &TupleDescData<'_>,
    rel: &Relation<'_>,
    attnumlist: &[i16],
    list: Option<&NodeList<'_>>,
    all: bool,
    optname: &str,
) -> PgResult<PgVec<'mcx, bool>> {
    let natts = tup_desc.natts as usize;
    let mut flags = vec_from_elem_in(mcx, false, natts);
    if all {
        for &attnum in attnumlist {
            flags[attnum as usize - 1] = true;
        }
        return Ok(flags);
    }
    let Some(list) = list else { return Ok(flags) };
    let attnums = CopyGetAttnums(mcx, tup_desc, rel, list)?;
    for &attnum in attnums.iter() {
        if !attnumlist.contains(&attnum) {
            let att = tup_desc.attr(attnum as usize - 1);
            return Err(Box::new(
                PgError::error(format!(
                    "{optname} column \"{}\" not referenced by COPY",
                    String::from_utf8_lossy(att.attname.name_str())
                ))
                .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE),
            ));
        }
        flags[attnum as usize - 1] = true;
    }
    Ok(flags)
}

// errorConflictingDefElem (define.c).
#[cold]
#[inline(never)]
fn conflicting_option(src: Option<&str>, location: types_core::ParseLoc) -> Box<PgError> {
    Box::new(
        PgError::error("conflicting or redundant options")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_cursor_position(errpos(src, location)),
    )
}

/// `CopyGetAttnums` (copy.c): 1-based attnums to copy.
pub fn CopyGetAttnums<'mcx>(
    mcx: Mcx<'mcx>,
    tup_desc: &TupleDescData<'_>,
    rel: &Relation<'_>,
    attnamelist: &NodeList<'_>,
) -> PgResult<PgVec<'mcx, i16>> {
    let mut attnums: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    if attnamelist.is_nil() {
        for i in 0..tup_desc.natts as usize {
            let attr = tup_desc.attr(i);
            if attr.attisdropped || attr.attgenerated != 0 {
                continue;
            }
            attnums.push(i as i16 + 1);
        }
        return Ok(attnums);
    }
    for l in attnamelist.iter() {
        let name = string_node(l);
        let mut attnum: i16 = 0;
        for i in 0..tup_desc.natts as usize {
            let att = tup_desc.attr(i);
            if att.attisdropped {
                continue;
            }
            if att.attname.name_str() == name.as_bytes() {
                if att.attgenerated != 0 {
                    return Err(Box::new(
                        PgError::error(format!("column \"{name}\" is a generated column"))
                            .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE)
                            .with_detail("Generated columns cannot be used in COPY."),
                    ));
                }
                attnum = att.attnum;
                break;
            }
        }
        if attnum == 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "column \"{name}\" of relation \"{}\" does not exist",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
            ));
        }
        if attnums.contains(&attnum) {
            return Err(Box::new(
                PgError::error(format!("column \"{name}\" specified more than once"))
                    .with_sqlstate(ERRCODE_DUPLICATE_COLUMN),
            ));
        }
        attnums.push(attnum);
    }
    Ok(attnums)
}

fn string_node<'a>(n: Node<'a>) -> &'a str {
    n.as_string().expect("attlist member is String").sval
}

#[cold]
#[inline(never)]
fn from_file_denied() -> Box<PgError> {
    Box::new(
        PgError::error("permission denied to COPY from a file")
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .with_detail(
                "Only roles with privileges of the \"pg_read_server_files\" role may COPY \
                 from a file.",
            )
            .with_hint(
                "Anyone can COPY to stdout or from stdin. psql's \\copy command also works \
                 for anyone.",
            ),
    )
}

#[cold]
#[inline(never)]
fn to_file_denied() -> Box<PgError> {
    Box::new(
        PgError::error("permission denied to COPY to a file")
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .with_detail(
                "Only roles with privileges of the \"pg_write_server_files\" role may COPY \
                 to a file.",
            )
            .with_hint(
                "Anyone can COPY to stdout or from stdin. psql's \\copy command also works \
                 for anyone.",
            ),
    )
}

pub fn init_seams() {}
