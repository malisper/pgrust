// copy.c/copyto.c/copyfrom.c/copyfromparse.c — text, CSV and binary formats,
// file and wire STDIN/STDOUT variants; column defaults, the DEFAULT marker,
// FROM ... WHERE, COPY (query) TO and the RLS TO->SELECT rewrite live. Loud
// (named): PROGRAM, HEADER match, volatile defaults/WHERE. Option parsing
// (ProcessCopyOptions) is full-parity.
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
mod fromparquet;
mod fromparse;
mod memheadroom;
mod parallel;
mod to;
#[cfg(test)]
mod tests;

pub use from::{copy_from_error_context, BeginCopyFrom, BeginCopyFromCallback, CopyFrom, CopyFromState, EndCopyFrom};
pub use to::{BeginCopyTo, DoCopyTo, EndCopyTo};
#[doc(hidden)]
pub use fromparse::bench_internals;
#[doc(hidden)]
pub use to::copy_attribute_out_text;

const ROLE_PG_READ_SERVER_FILES: Oid = 4569;
const ROLE_PG_WRITE_SERVER_FILES: Oid = 4570;

const ACL_INSERT: u64 = 1 << 0;
const ACL_SELECT: u64 = 1 << 1;

const RELKIND_RELATION: u8 = b'r';

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: COPY {what}")
}

// CopyHeaderChoice (copy.h).
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyHeaderChoice {
    #[default]
    False,
    True,
    Match,
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
    /// COPY FROM ... WITH (FORMAT 'parquet') — read-only surface, matching
    /// pg_parquet's spelling. COPY TO is refused.
    pub parquet: bool,
    /// MATCH_BY 'name' (parquet only; default is positional matching).
    pub parquet_match_by_name: bool,
    /// COERCE_EPOCH (parquet only, default off): plain-integer columns
    /// bound to timestamp/timestamptz (or date) targets are read as Unix
    /// epoch seconds (respectively epoch days).
    pub parquet_coerce_epoch: bool,
    pub freeze: bool,
    pub delim: u8,
    pub quote: u8,
    pub escape: u8,
    pub null_print: &'s str,
    pub default_print: Option<&'s str>,
    pub header_line: CopyHeaderChoice,
    pub force_quote: Option<&'s NodeList<'s>>,
    pub force_quote_all: bool,
    pub force_notnull: Option<&'s NodeList<'s>>,
    pub force_notnull_all: bool,
    pub force_null: Option<&'s NodeList<'s>>,
    pub force_null_all: bool,
    pub convert_selectively: bool,
    pub convert_select: Option<&'s NodeList<'s>>,
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
pub fn DoCopy<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CopyStmt<'mcx>,
    source_text: &str,
    stmt_location: types_core::ParseLoc,
    stmt_len: types_core::ParseLoc,
) -> PgResult<u64> {
    let is_from = stmt.is_from;
    if stmt.is_program {
        // unported: COPY TO/FROM PROGRAM (OpenPipeStream lane)
        return Err(Box::new(
            PgError::error("COPY TO/FROM PROGRAM is not supported yet".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
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
        assert!(!is_from, "COPY (query) FROM is excluded by the grammar");
        let raw_query = types_nodes::rawnodes::RawStmt {
            stmt: Some(stmt.query.expect("CopyStmt without relation has a query")),
            stmt_location,
            stmt_len,
        };
        let mut cstate = BeginCopyTo(
            mcx,
            None,
            Some(&raw_query),
            types_core::InvalidOid,
            stmt.filename,
            &stmt.attlist,
            &stmt.options,
            Some(source_text),
        )?;
        let processed = DoCopyTo(mcx, &mut cstate, None)?;
        EndCopyTo(cstate)?;
        return Ok(processed);
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

    // C DoCopy builds a one-relation perminfo and runs ExecCheckPermissions
    // on it, so a column-list COPY passes on column-level privileges alone.
    let mut perminfo = types_nodes::parsenodes::RTEPermissionInfo {
        relid: rel.rd_id,
        requiredPerms: if is_from { ACL_INSERT } else { ACL_SELECT },
        ..Default::default()
    };
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
        let where_perminfo = nsitem.p_perminfo;
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
        // Stored generated columns are not yet computed when the WHERE
        // filter runs; virtual kept consistent with stored (copy.c:173-185).
        let mut attnos = types_nodes::Bitmapset::default();
        vars::pull_varattnos(mcx, qual, 1, &mut attnos)?;
        // A whole-row reference examines every column (copy.c:156-163).
        const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
        if attnos.is_member(-FLIHAN) {
            attnos.add_range(mcx, 1 - FLIHAN, rel.rd_att.natts as i32 - FLIHAN)?;
            attnos.del_member(-FLIHAN);
        }
        for m in attnos.iter() {
            let attno = m + FLIHAN;
            if attno <= 0 {
                continue;
            }
            if rel.rd_att.attr(attno as usize - 1).attgenerated != 0 {
                let name =
                    lsyscache::attribute::get_attname(mcx, rel.rd_id, attno as i16, false)?
                        .expect("checked missing_ok=false");
                return Err(Box::new(
                    PgError::error(
                        "generated columns are not supported in COPY FROM WHERE conditions",
                    )
                    .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE)
                    .with_detail(format!("Column \"{name}\" is a generated column.")),
                ));
            }
        }
        // In C the WHERE transform marks Vars for SELECT privilege on the
        // same perminfo (markVarForSelectPriv); fold the pstate copy in.
        if let Some(wpin) = where_perminfo {
            let wpi = wpin.as_rte_permission_info().expect("p_perminfo is RTEPermissionInfo");
            if !wpi.selectedCols.is_empty() {
                perminfo.requiredPerms |= ACL_SELECT;
                perminfo.selectedCols.add_members(mcx, &wpi.selectedCols)?;
            }
        }
        let qual = clauses::eval_const_expressions(mcx, qual)?;
        let qual = planner::prepqual::canonicalize_qual(mcx, qual, false)?;
        where_clause = clauses::make_ands_implicit(mcx, Some(qual))?;
        parser_small1::free_parsestate(pstate)?;
    }
    {
        const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
        let attnums = CopyGetAttnums(mcx, &rel.rd_att, Some(&rel), &stmt.attlist)?;
        let bms =
            if is_from { &mut perminfo.insertedCols } else { &mut perminfo.selectedCols };
        for &attno in attnums.iter() {
            bms.add_member(mcx, attno as i32 - FLIHAN)?;
        }
    }
    execmain_seams::exec_check_permissions::call(&NodeList::make1(
        mcx,
        Node::mk(mcx, perminfo)?,
    )?)?;
    if rls::check_enable_rls(rel.rd_id, types_core::InvalidOid, false)?
        == rls::CheckEnableRls::RlsEnabled
    {
        if is_from {
            return Err(Box::new(
                PgError::error("COPY FROM not supported with row-level security".to_string())
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_hint("Use INSERT statements instead."),
            ));
        }
        // COPY rel TO under RLS becomes COPY (SELECT ...) FROM ONLY rel TO so
        // the rewriter adds the policy quals (copy.c DoCopy RLS branch).
        use types_nodes::rawnodes::{A_Star, ColumnRef, ResTarget, SelectStmt};
        let mk_target = |val: Node<'mcx>| -> PgResult<Node<'mcx>> {
            Node::mk(
                mcx,
                ResTarget { name: None, indirection: NodeList::nil(), val: Some(val), location: -1 },
            )
        };
        let target_list = if stmt.attlist.is_nil() {
            let cr = Node::mk(
                mcx,
                ColumnRef { fields: NodeList::make1(mcx, Node::mk(mcx, A_Star)?)?, location: -1 },
            )?;
            NodeList::make1(mcx, mk_target(cr)?)?
        } else {
            let mut targets: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
            for col in stmt.attlist.iter() {
                let cr = Node::mk(
                    mcx,
                    ColumnRef { fields: NodeList::make1(mcx, col)?, location: -1 },
                )?;
                targets.push(mk_target(cr)?);
            }
            NodeList::from_slice(mcx, &targets)?
        };
        let nspname = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?
            .expect("open relation has a namespace");
        let nspname = core::str::from_utf8(nspname.into_bytes().leak()).expect("PgString is UTF-8");
        let relname = mcx::PgString::from_str_in(rel.name(), mcx)?;
        let relname = core::str::from_utf8(relname.into_bytes().leak()).expect("PgString is UTF-8");
        // inh=false: ONLY, so COPY reads just the target table, as when RLS
        // doesn't apply.
        let from = Node::mk(
            mcx,
            types_nodes::primnodes::RangeVar {
                catalogname: None,
                schemaname: Some(nspname),
                relname: Some(relname),
                inh: false,
                relpersistence: b'p',
                alias: None,
                location: -1,
            },
        )?;
        let select = Node::mk(
            mcx,
            SelectStmt {
                targetList: target_list,
                fromClause: NodeList::make1(mcx, from)?,
                ..Default::default()
            },
        )?;
        let raw_query =
            types_nodes::rawnodes::RawStmt { stmt: Some(select), stmt_location, stmt_len };
        let query_rel_id = rel.rd_id;
        // C closes the relation here but keeps the lock until end of xact;
        // the query-based COPY reopens it.
        table::table_close(rel, types_rel::lock::NoLock)?;
        let mut cstate = BeginCopyTo(
            mcx,
            None,
            Some(&raw_query),
            query_rel_id,
            stmt.filename,
            &stmt.attlist,
            &stmt.options,
            Some(source_text),
        )?;
        let processed = DoCopyTo(mcx, &mut cstate, None)?;
        EndCopyTo(cstate)?;
        return Ok(processed);
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
            Some(&rel),
            None,
            types_core::InvalidOid,
            stmt.filename,
            &stmt.attlist,
            &stmt.options,
            Some(source_text),
        )?;
        let processed = DoCopyTo(mcx, &mut cstate, Some(&rel))?;
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

// defGetCopyHeaderChoice (copy.c).
fn def_header_choice(
    d: &types_nodes::parsenodes::DefElem<'_>,
    is_from: bool,
) -> PgResult<CopyHeaderChoice> {
    let Some(arg) = d.arg else { return Ok(CopyHeaderChoice::True) };
    if let Some(i) = arg.as_integer() {
        match i.ival {
            0 => return Ok(CopyHeaderChoice::False),
            1 => return Ok(CopyHeaderChoice::True),
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
            return Ok(CopyHeaderChoice::True);
        }
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(CopyHeaderChoice::False);
        }
        if sval.eq_ignore_ascii_case("match") {
            if !is_from {
                return Err(Box::new(
                    PgError::error(format!("cannot use \"{sval}\" with HEADER in COPY TO"))
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            return Ok(CopyHeaderChoice::Match);
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

// defGetCopyRejectLimitOption (copy.c); the T_Float defGetInt64 arm is loud.
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
        Some(n) => match (n.as_integer(), n.as_string()) {
            (Some(i), _) => i.ival as i64,
            // The reloptions form (file_fdw): pg_strtoint64 over the text.
            (None, Some(s)) => numutils::pg_strtoint64(s.sval)?,
            (None, None) => panic!(
                "defGetCopyRejectLimitOption (copy.c): T_Float REJECT_LIMIT arm \
                 (defGetInt64) not ported"
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

#[track_caller]
#[cold]
#[inline(never)]
fn requires_csv(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("COPY {name} requires CSV mode"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_in_binary(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot specify {name} in BINARY mode"))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_in_parquet(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot specify {name} with parquet format"))
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
        parquet: false,
        parquet_match_by_name: false,
        parquet_coerce_epoch: false,
        freeze: false,
        delim: 0,
        quote: 0,
        escape: 0,
        null_print: "",
        default_print: None,
        header_line: CopyHeaderChoice::False,
        force_quote: None,
        force_quote_all: false,
        force_notnull: None,
        force_notnull_all: false,
        force_null: None,
        force_null_all: false,
        convert_selectively: false,
        convert_select: None,
        on_error: CopyOnErrorChoice::Stop,
        log_verbosity: CopyLogVerbosityChoice::Default,
        reject_limit: 0,
    };
    let mut format_specified = false;
    let mut match_by_specified = false;
    let mut coerce_epoch_specified = false;
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
                    "parquet" => opts.parquet = true,
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
            "convert_selectively" => {
                if opts.convert_selectively {
                    return Err(conflicting_option(src, d.location));
                }
                opts.convert_selectively = true;
                opts.convert_select = d.arg.and_then(|a| a.as_list());
                if !(d.arg.is_none() || d.arg.is_some_and(|a| a.as_list().is_some())) {
                    return Err(Box::new(
                        PgError::error(format!(
                            "argument to option \"{name}\" must be a list of column names"
                        ))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                        .with_cursor_position(errpos(src, d.location)),
                    ));
                }
            }
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
            // pg_parquet's FROM-side column-matching option.
            "match_by" => {
                if match_by_specified {
                    return Err(conflicting_option(src, d.location));
                }
                match_by_specified = true;
                match def_string(d)? {
                    "position" => opts.parquet_match_by_name = false,
                    "name" => opts.parquet_match_by_name = true,
                    sval => {
                        return Err(Box::new(
                            PgError::error(format!(
                                "COPY MATCH_BY \"{sval}\" not recognized"
                            ))
                            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                            .with_cursor_position(errpos(src, d.location)),
                        ))
                    }
                }
            }
            // Opt-in epoch coercion for the parquet reader (FROM side).
            "coerce_epoch" => {
                if coerce_epoch_specified {
                    return Err(conflicting_option(src, d.location));
                }
                coerce_epoch_specified = true;
                opts.parquet_coerce_epoch = def_boolean(d)?;
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

    if opts.parquet {
        // Read-only surface (product ruling): the writer is not planned.
        if !is_from {
            return Err(Box::new(
                PgError::error("COPY TO with parquet format is not supported")
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        // Typed self-describing input: none of the text-shape options apply.
        if delim.is_some() {
            return Err(cannot_in_parquet("DELIMITER"));
        }
        if null_print.is_some() {
            return Err(cannot_in_parquet("NULL"));
        }
        if opts.default_print.is_some() {
            return Err(cannot_in_parquet("DEFAULT"));
        }
        if opts.header_line != CopyHeaderChoice::False {
            return Err(cannot_in_parquet("HEADER"));
        }
        if quote.is_some() {
            return Err(cannot_in_parquet("QUOTE"));
        }
        if escape.is_some() {
            return Err(cannot_in_parquet("ESCAPE"));
        }
        if opts.force_quote.is_some() || opts.force_quote_all {
            return Err(cannot_in_parquet("FORCE_QUOTE"));
        }
        if opts.force_notnull.is_some() || opts.force_notnull_all {
            return Err(cannot_in_parquet("FORCE_NOT_NULL"));
        }
        if opts.force_null.is_some() || opts.force_null_all {
            return Err(cannot_in_parquet("FORCE_NULL"));
        }
        if opts.convert_selectively {
            return Err(cannot_in_parquet("convert_selectively"));
        }
        if opts.file_encoding >= 0 {
            return Err(cannot_in_parquet("ENCODING"));
        }
        if opts.on_error != CopyOnErrorChoice::Stop {
            return Err(Box::new(
                PgError::error("only ON_ERROR STOP is allowed with parquet format")
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ));
        }
    } else if match_by_specified {
        return Err(Box::new(
            PgError::error("COPY MATCH_BY requires parquet format")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    } else if coerce_epoch_specified {
        return Err(Box::new(
            PgError::error("COPY COERCE_EPOCH requires parquet format")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
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
    if opts.binary && opts.header_line != CopyHeaderChoice::False {
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
    rel: Option<&Relation<'_>>,
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
#[track_caller]
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
    rel: Option<&Relation<'_>>,
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
            let msg = match rel {
                Some(rel) => format!(
                    "column \"{name}\" of relation \"{}\" does not exist",
                    rel.name()
                ),
                None => format!("column \"{name}\" does not exist"),
            };
            return Err(Box::new(
                PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
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

#[track_caller]
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

#[track_caller]
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

pub fn init_seams() {
    copy_seams::copy_dest_receive::set(to::copy_dest_receive);
}
