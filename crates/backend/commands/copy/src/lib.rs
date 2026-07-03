// copy.c/copyto.c/copyfrom.c/copyfromparse.c — text + CSV formats, file and
// wire STDIN/STDOUT variants. Loud (named): binary format, COPY (query),
// WHERE, PROGRAM, ON_ERROR ignore, FREEZE, HEADER match, defaults/generated
// columns, RLS rewrite.
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

pub struct CopyFormatOptions<'s> {
    pub file_encoding: i32,
    pub csv_mode: bool,
    pub delim: u8,
    pub quote: u8,
    pub escape: u8,
    pub null_print: &'s str,
    pub header_line: bool,
    pub force_quote: Option<&'s NodeList<'s>>,
    pub force_quote_all: bool,
    pub force_notnull: Option<&'s NodeList<'s>>,
    pub force_notnull_all: bool,
    pub force_null: Option<&'s NodeList<'s>>,
    pub force_null_all: bool,
}

/// `DoCopy` (copy.c). Returns rows processed.
pub fn DoCopy<'mcx>(mcx: Mcx<'mcx>, stmt: &CopyStmt<'_>) -> PgResult<u64> {
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
        unported("(query) TO (pg_analyze_and_rewrite + executor lane)");
    };
    if stmt.whereClause.is_some() {
        unported("FROM ... WHERE (transformExpr/ExecQual lane)");
    }
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

    let processed = if is_from {
        if xact::XactReadOnly() && !rel.rd_islocaltemp {
            xact::PreventCommandIfReadOnly("COPY FROM")?;
        }
        let mut cstate = BeginCopyFrom(mcx, &rel, stmt.filename, &stmt.attlist, &stmt.options)?;
        let processed = CopyFrom(mcx, &mut cstate, &rel)?;
        EndCopyFrom(cstate)?;
        processed
    } else {
        let mut cstate = BeginCopyTo(mcx, &rel, stmt.filename, &stmt.attlist, &stmt.options)?;
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
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    ))
}

// defGetCopyHeaderChoice (copy.c); COPY_HEADER_MATCH is loud.
fn def_header_choice(d: &types_nodes::parsenodes::DefElem<'_>, is_from: bool) -> PgResult<bool> {
    let Some(arg) = d.arg else { return Ok(true) };
    if let Some(i) = arg.as_integer() {
        return Ok(i.ival != 0);
    }
    if let Some(b) = arg.as_boolean() {
        return Ok(b.boolval);
    }
    if let Some(s) = arg.as_string() {
        match s.sval {
            "true" | "on" | "1" => return Ok(true),
            "false" | "off" | "0" => return Ok(false),
            "match" => {
                if !is_from {
                    return Err(Box::new(
                        PgError::error("cannot use \"match\" with HEADER in COPY TO")
                            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                unported("HEADER match");
            }
            _ => {}
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

#[cold]
#[inline(never)]
fn requires_csv(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("COPY {name} requires CSV mode"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

/// `ProcessCopyOptions` (copy.c), text + CSV; binary loud.
pub fn ProcessCopyOptions<'s>(
    is_from: bool,
    options: &NodeList<'s>,
) -> PgResult<CopyFormatOptions<'s>> {
    let mut opts = CopyFormatOptions {
        file_encoding: -1,
        csv_mode: false,
        delim: 0,
        quote: 0,
        escape: 0,
        null_print: "",
        header_line: false,
        force_quote: None,
        force_quote_all: false,
        force_notnull: None,
        force_notnull_all: false,
        force_null: None,
        force_null_all: false,
    };
    let mut format_specified = false;
    let mut header_specified = false;
    let mut delim: Option<&str> = None;
    let mut null_print: Option<&str> = None;
    let mut quote: Option<&str> = None;
    let mut escape: Option<&str> = None;
    let mut force_quote_specified = false;
    let mut force_notnull_specified = false;
    let mut force_null_specified = false;

    for option in options.iter() {
        let d = option.as_def_elem().expect("COPY options: DefElem list");
        let name = d.defname.unwrap_or("");
        match name {
            "format" => {
                if format_specified {
                    return Err(conflicting_option(name));
                }
                format_specified = true;
                match def_string(d)? {
                    "text" => {}
                    "csv" => opts.csv_mode = true,
                    fmt @ "binary" => unported_fmt(fmt),
                    fmt => {
                        return Err(Box::new(
                            PgError::error(format!("COPY format \"{fmt}\" not recognized"))
                                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                        ))
                    }
                }
            }
            "delimiter" => {
                if delim.is_some() {
                    return Err(conflicting_option(name));
                }
                delim = Some(def_string(d)?);
            }
            "null" => {
                if null_print.is_some() {
                    return Err(conflicting_option(name));
                }
                null_print = Some(def_string(d)?);
            }
            "header" => {
                if header_specified {
                    return Err(conflicting_option(name));
                }
                header_specified = true;
                opts.header_line = def_header_choice(d, is_from)?;
            }
            "quote" => {
                if quote.is_some() {
                    return Err(conflicting_option(name));
                }
                quote = Some(def_string(d)?);
            }
            "escape" => {
                if escape.is_some() {
                    return Err(conflicting_option(name));
                }
                escape = Some(def_string(d)?);
            }
            "force_quote" => {
                if force_quote_specified {
                    return Err(conflicting_option(name));
                }
                force_quote_specified = true;
                (opts.force_quote, opts.force_quote_all) = def_list_or_star(d)?;
            }
            "force_not_null" => {
                if force_notnull_specified {
                    return Err(conflicting_option(name));
                }
                force_notnull_specified = true;
                (opts.force_notnull, opts.force_notnull_all) = def_list_or_star(d)?;
            }
            "force_null" => {
                if force_null_specified {
                    return Err(conflicting_option(name));
                }
                force_null_specified = true;
                (opts.force_null, opts.force_null_all) = def_list_or_star(d)?;
            }
            "encoding" => {
                if opts.file_encoding >= 0 {
                    return Err(conflicting_option(name));
                }
                opts.file_encoding = mbutils::pg_char_to_encoding(def_string(d)?);
                if opts.file_encoding < 0 {
                    return Err(Box::new(
                        PgError::error(format!(
                            "argument to option \"{name}\" must be a valid encoding name"
                        ))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                    ));
                }
            }
            "freeze" => unported("FREEZE (multi-insert/frozen lane)"),
            "on_error" => {
                if !is_from {
                    return Err(Box::new(
                        PgError::error("COPY ON_ERROR cannot be used with COPY TO")
                            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                    ));
                }
                if def_string(d)?.eq_ignore_ascii_case("stop") {
                    continue;
                }
                unported("ON_ERROR ignore (soft-error skip lane)");
            }
            "default" => unported("DEFAULT marker (defaults rewrite gap)"),
            "log_verbosity" | "reject_limit" => unported("ON_ERROR companions"),
            "convert_selectively" => unported("convert_selectively"),
            other => {
                return Err(Box::new(
                    PgError::error(format!("option \"{other}\" not recognized"))
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        }
    }

    let delim = delim.unwrap_or(if opts.csv_mode { "," } else { "\t" });
    opts.null_print = null_print.unwrap_or(if opts.csv_mode { "" } else { "\\N" });
    if opts.csv_mode {
        let quote = quote.unwrap_or("\"");
        let escape = escape.unwrap_or(quote);
        if quote.len() != 1 {
            return Err(Box::new(
                PgError::error("COPY quote must be a single one-byte character")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if escape.len() != 1 {
            return Err(Box::new(
                PgError::error("COPY escape must be a single one-byte character")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        opts.quote = quote.as_bytes()[0];
        opts.escape = escape.as_bytes()[0];
    } else {
        if quote.is_some() {
            return Err(requires_csv("QUOTE"));
        }
        if escape.is_some() {
            return Err(requires_csv("ESCAPE"));
        }
    }

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
    if !opts.csv_mode && b"\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(&opts.delim) {
        return Err(Box::new(
            PgError::error(format!("COPY delimiter cannot be \"{}\"", opts.delim as char))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if opts.csv_mode && opts.delim == opts.quote {
        return Err(Box::new(
            PgError::error("COPY delimiter and quote must be different")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
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

#[cold]
#[inline(never)]
fn unported_fmt(fmt: &str) -> ! {
    panic!("unported: COPY FORMAT {fmt} (text-only lane)")
}

#[cold]
#[inline(never)]
fn conflicting_option(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("conflicting or redundant options: {name}"))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
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
