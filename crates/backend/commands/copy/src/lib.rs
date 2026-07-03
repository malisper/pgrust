// copy.c/copyto.c/copyfrom.c/copyfromparse.c — text format, file variant.
// Loud (named): CSV/binary formats, COPY (query), WHERE, PROGRAM, the wire
// STDIN/STDOUT subprotocol (extended-query lane), ON_ERROR ignore, FREEZE,
// HEADER match, defaults/generated columns, RLS rewrite.
#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
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
    pub delim: u8,
    pub null_print: &'s str,
    pub header_line: bool,
}

/// `DoCopy` (copy.c). Returns rows processed.
pub fn DoCopy<'mcx>(mcx: Mcx<'mcx>, stmt: &CopyStmt<'_>) -> PgResult<u64> {
    let is_from = stmt.is_from;
    if stmt.is_program {
        unported("TO/FROM PROGRAM (OpenPipeStream lane)");
    }
    let Some(filename) = stmt.filename else {
        unported("STDIN/STDOUT wire subprotocol (extended-query lane)");
    };

    let userid = miscinit_seams::get_user_id::call();
    let (role, denied) = if is_from {
        (ROLE_PG_READ_SERVER_FILES, from_file_denied as fn() -> Box<PgError>)
    } else {
        (ROLE_PG_WRITE_SERVER_FILES, to_file_denied as fn() -> Box<PgError>)
    };
    if !acl_seams::has_privs_of_role::call(userid, role)? {
        return Err(denied());
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

    // ExecCheckPermissions, relation-level arm (execmain precedent): the
    // column-level fallback is loud.
    let required = if is_from { ACL_INSERT } else { ACL_SELECT };
    let r = aclchk_seams::object_aclcheck::call(
        types_core::catalog::RELATION_RELATION_ID,
        rel.rd_id,
        userid,
        required,
    )?;
    if r != ACLCHECK_OK {
        panic!(
            "DoCopy (copy.c): relation-level access denied for relation {} — \
             column-level aclcheck fallback and aclcheck_error not ported",
            rel.rd_id
        );
    }
    if rel.rd_rel.relrowsecurity {
        unported("with row-level security (query-rewrite arm)");
    }

    let processed = if is_from {
        if xact::XactReadOnly() && !rel.rd_islocaltemp {
            xact::PreventCommandIfReadOnly("COPY FROM")?;
        }
        let mut cstate = BeginCopyFrom(mcx, &rel, filename, &stmt.attlist, &stmt.options)?;
        let processed = CopyFrom(mcx, &mut cstate, &rel)?;
        EndCopyFrom(cstate)?;
        processed
    } else {
        let mut cstate = BeginCopyTo(mcx, &rel, filename, &stmt.attlist, &stmt.options)?;
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

fn def_bool(d: &types_nodes::parsenodes::DefElem<'_>) -> PgResult<bool> {
    let Some(arg) = d.arg else { return Ok(true) };
    if let Some(b) = arg.as_boolean() {
        return Ok(b.boolval);
    }
    if let Some(s) = arg.as_string() {
        match s.sval {
            "true" | "on" => return Ok(true),
            "false" | "off" => return Ok(false),
            _ => {}
        }
    }
    Err(Box::new(
        PgError::error(format!(
            "{} requires a Boolean value",
            d.defname.unwrap_or("")
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    ))
}

/// `ProcessCopyOptions` (copy.c), text-format subset; CSV/binary loud.
pub fn ProcessCopyOptions<'s>(
    is_from: bool,
    options: &NodeList<'s>,
) -> PgResult<CopyFormatOptions<'s>> {
    let mut opts = CopyFormatOptions {
        file_encoding: -1,
        delim: 0,
        null_print: "",
        header_line: false,
    };
    let mut format_specified = false;
    let mut header_specified = false;
    let mut delim: Option<&str> = None;
    let mut null_print: Option<&str> = None;

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
                    fmt @ ("csv" | "binary") => unported_fmt(fmt),
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
                if d.arg
                    .is_some_and(|a| a.as_string().is_some_and(|s| s.sval == "match"))
                {
                    if !is_from {
                        return Err(Box::new(
                            PgError::error("cannot use \"match\" with HEADER in COPY TO")
                                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ));
                    }
                    unported("HEADER match");
                }
                opts.header_line = def_bool(d)?;
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
            "quote" | "escape" | "force_quote" | "force_not_null" | "force_null" => {
                return Err(Box::new(
                    PgError::error(format!("COPY {} requires CSV mode", name.to_uppercase()))
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ))
            }
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

    let delim = delim.unwrap_or("\t");
    opts.null_print = null_print.unwrap_or("\\N");

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
    if b"\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(&opts.delim) {
        return Err(Box::new(
            PgError::error(format!("COPY delimiter cannot be \"{}\"", opts.delim as char))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if opts.null_print.as_bytes().contains(&opts.delim) {
        return Err(Box::new(
            PgError::error("COPY delimiter character must not appear in the NULL specification")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(opts)
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
