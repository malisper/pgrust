//! `utils/adt/ruleutils.c` — the view- and rule-definition deparsers
//! (`pg_get_viewdef` / `pg_get_ruledef`, the `pg_get_viewdef_worker` /
//! `pg_get_ruledef_worker` bodies and `make_viewdef` / `make_ruledef`,
//! ruleutils.c 559-870, 5342-5586).
//!
//! # Status: ported end-to-end for the common SELECT-view spine
//!
//! `pg_get_viewdef(viewoid)` reconstructs the SELECT body of a view by reading
//! the view's `_RETURN` rule out of `pg_rewrite`, `stringToNode`-ing the stored
//! `ev_action` `pg_node_tree` into the action `Query`, and deparsing it through
//! the ported [`crate::query_deparse::get_query_def`] engine. The result column
//! labels come from the view relation's tuple descriptor
//! (`RelationGetDescr(ev_relation)`), so `make_viewdef`'s `resultDesc` argument
//! is wired.
//!
//! C reads `pg_rewrite` over SPI (so the planner's read-access check on
//! `pg_rewrite` fires). The owned model reads it with the same MVCC catalog
//! scan the relcache rule builder uses — `relcache_scan_pg_rewrite(ev_class)`
//! (genam) returns every rule on the relation in `rulename` order; the view's
//! `_RETURN` rule is the unique unconditional `INSTEAD SELECT` rule (`ev_type ==
//! '1'`, `is_instead`, `ev_qual == "<>"`), which is exactly the filter
//! `make_viewdef` itself applies after the by-name fetch (ruleutils.c 5566). The
//! read-access ACL check on `pg_rewrite` (the only behavioral reason C uses SPI
//! here) is not modeled.
//!
//! `pg_get_ruledef(ruleoid)` is the by-OID sibling: it fetches the single
//! `pg_rewrite` row by rule OID and runs [`make_ruledef`], which renders the
//! full `CREATE RULE ... AS ON <event> TO <relation> [WHERE ...] DO [INSTEAD]
//! (<actions>)` text.

use alloc::format;
use alloc::vec::Vec;
use mcx::{Mcx, PgBox, PgString, PgVec};
use ::types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use ::nodes::nodes::{ntag, CmdType, Node};
use ::types_storage::lock::AccessShareLock;

use crate::{PRETTYFLAG_INDENT, PRETTYFLAG_SCHEMA, WRAP_COLUMN_DEFAULT};

/// `ViewSelectRuleName` (rewriteDefine.h): the conventional name of a view's
/// auto-generated `INSTEAD SELECT` rule.
const VIEW_SELECT_RULE_NAME: &str = "_RETURN";

/// `PRETTYINDENT_STD` (ruleutils.c 87).
const PRETTYINDENT_STD: i32 = 8;

/// `make_viewdef`'s SELECT-rule predicate (ruleutils.c 5558-5566): the action
/// must be a single `CMD_SELECT` query installed by an unconditional
/// `INSTEAD SELECT` rule. `relcache_scan_pg_rewrite` does not surface the
/// `rulename`, but the `_RETURN` rule is the unique row matching this predicate,
/// so the filter is equivalent to the by-name `(ev_class, '_RETURN')` fetch.
fn is_view_select_rule(r: &genam_seams::ScannedPgRewrite) -> bool {
    r.ev_type == b'1'
        && r.is_instead
        && r.ev_qual.as_deref() == Some("<>")
}

/// `pg_get_viewdef_worker(viewoid, prettyFlags, wrapColumn)` (ruleutils.c
/// 788-861). Returns the deparsed SELECT body (`Ok(Some(text))`), or `Ok(None)`
/// when there is no matching `_RETURN` rule (C: the empty output buffer).
pub fn pg_get_viewdef_worker<'mcx>(
    mcx: Mcx<'mcx>,
    viewoid: Oid,
    pretty_flags: i32,
    wrap_column: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // C reads the `(ev_class, '_RETURN')` pg_rewrite row over SPI; we use the
    // same MVCC scan the relcache rule builder uses (scan by ev_class, then
    // filter to the unique `_RETURN` SELECT-INSTEAD rule below).
    let rules = genam_seams::relcache_scan_pg_rewrite::call(viewoid)?;

    let ruletup = match rules.iter().find(|r| is_view_select_rule(r)) {
        // SPI_processed != 1 -> keep the output buffer empty and leave (NULL).
        None => return Ok(None),
        Some(r) => r,
    };

    make_viewdef(mcx, ruletup, viewoid, pretty_flags, wrap_column)
}

/// `make_viewdef(buf, ruletup, rulettc, prettyFlags, wrapColumn)` (ruleutils.c
/// 5538-5586). Given the view's `_RETURN` rule, `stringToNode` its `ev_action`
/// into the action `Query`, open the view relation for its result tuple
/// descriptor, and deparse the query. Returns `Ok(None)` when the action list
/// is not a single SELECT query (C: keep output buffer empty and leave).
fn make_viewdef<'mcx>(
    mcx: Mcx<'mcx>,
    ruletup: &genam_seams::ScannedPgRewrite,
    ev_class: Oid,
    pretty_flags: i32,
    wrap_column: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // actions = (List *) stringToNode(ev_action);
    let action_node = match &ruletup.ev_action {
        Some(text) => read_seams::string_to_node_opt::call(mcx, text.as_str())?,
        None => None,
    };

    // if (list_length(actions) != 1) keep output buffer empty and leave.
    let query = match single_action_query(mcx, action_node)? {
        None => return Ok(None),
        Some(q) => q,
    };

    // if (ev_type != '1' || !is_instead || strcmp(ev_qual, "<>") != 0 ||
    //     query->commandType != CMD_SELECT) keep buffer empty and leave.
    // (ev_type/is_instead/ev_qual already matched by is_view_select_rule.)
    if query.commandType != CmdType::CMD_SELECT {
        return Ok(None);
    }

    // ev_relation = table_open(ev_class, AccessShareLock);
    let ev_relation =
        common_relation_seams::relation_open::call(mcx, ev_class, AccessShareLock)?;

    // get_query_def(query, buf, NIL, RelationGetDescr(ev_relation), true,
    //               prettyFlags, wrapColumn, 0);
    let result_desc = ev_relation.rd_att_clone_in(mcx)?;
    let buf = stringinfo::StringInfo::new_in(mcx);
    let no_parent: [crate::DeparseNamespace<'mcx>; 0] = [];
    let buf = crate::query_deparse::get_query_def(
        mcx,
        &query,
        buf,
        &no_parent,
        Some(result_desc),
        true,
        pretty_flags,
        wrap_column,
        0,
    )?;

    // table_close(ev_relation, AccessShareLock);
    ev_relation.close(AccessShareLock)?;

    // appendStringInfoChar(buf, ';');
    let mut buf = buf;
    append_char(mcx, &mut buf, b';')?;

    Ok(Some(buf_to_string(mcx, buf)?))
}

/// `pg_get_ruledef_worker(ruleoid, prettyFlags)` (ruleutils.c 596-666). Fetch
/// the single `pg_rewrite` row by rule OID and render the `CREATE RULE` text.
/// `Ok(None)` when the rule OID is gone (C: SPI_processed != 1 -> empty output).
pub fn pg_get_ruledef_worker<'mcx>(
    mcx: Mcx<'mcx>,
    ruleoid: Oid,
    pretty_flags: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // The C SPI query is `SELECT * FROM pg_rewrite WHERE oid = $1`. We read the
    // by-OID pg_rewrite row through the catalog by-OID projection seam, which
    // returns the same scalar columns plus the two node-string columns the
    // renderer needs.
    let row = match genam_seams::rule_by_oid::call(mcx, ruleoid)? {
        None => return Ok(None),
        Some(r) => r,
    };

    make_ruledef(mcx, &row, pretty_flags)
}

/// `make_ruledef(buf, ruletup, rulettc, prettyFlags)` (ruleutils.c 5347-5530).
fn make_ruledef<'mcx>(
    mcx: Mcx<'mcx>,
    ruletup: &genam_seams::RuleByOid,
    pretty_flags: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // actions = (List *) stringToNode(ev_action);
    let action_node = match &ruletup.ev_action {
        Some(text) => read_seams::string_to_node_opt::call(mcx, text.as_str())?,
        None => None,
    };
    let actions = action_list_queries(mcx, action_node)?;
    if actions.is_empty() {
        // elog(ERROR, "invalid empty ev_action list");
        return Err(PgError::error("invalid empty ev_action list"));
    }

    // ev_relation = table_open(ev_class, AccessShareLock);
    let ev_relation = common_relation_seams::relation_open::call(
        mcx,
        ruletup.ev_class,
        AccessShareLock,
    )?;

    let mut buf = stringinfo::StringInfo::new_in(mcx);

    // appendStringInfo(buf, "CREATE RULE %s AS", quote_identifier(rulename));
    let qrn = crate::quote_identifier(mcx, &ruletup.rulename)?;
    append_str(mcx, &mut buf, "CREATE RULE ")?;
    append_str(mcx, &mut buf, qrn.as_str())?;
    append_str(mcx, &mut buf, " AS")?;

    if (pretty_flags & PRETTYFLAG_INDENT) != 0 {
        append_str(mcx, &mut buf, "\n    ON ")?;
    } else {
        append_str(mcx, &mut buf, " ON ")?;
    }

    // The event the rule is fired for; the SELECT rule also feeds resultDesc.
    let mut view_result_desc: Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>> =
        None;
    match ruletup.ev_type {
        b'1' => {
            append_str(mcx, &mut buf, "SELECT")?;
            view_result_desc = Some(ev_relation.rd_att_clone_in(mcx)?);
        }
        b'2' => append_str(mcx, &mut buf, "UPDATE")?,
        b'3' => append_str(mcx, &mut buf, "INSERT")?,
        b'4' => append_str(mcx, &mut buf, "DELETE")?,
        other => {
            ev_relation.close(AccessShareLock)?;
            return Err(PgError::error(format!(
                "rule \"{}\" has unsupported event type {}",
                ruletup.rulename, other as i32
            ))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }

    // appendStringInfo(buf, " TO %s",
    //     (prettyFlags & PRETTYFLAG_SCHEMA) ? generate_relation_name(ev_class, NIL)
    //                                       : generate_qualified_relation_name(ev_class));
    let need_qual = (pretty_flags & PRETTYFLAG_SCHEMA) == 0;
    let relname =
        ruleutils_seams::generate_relation_name::call(mcx, ruletup.ev_class, need_qual)?;
    append_str(mcx, &mut buf, " TO ")?;
    append_str(mcx, &mut buf, relname.as_str())?;

    // If the rule has an event qualification, add it.
    if ruletup.ev_qual.as_deref() != Some("<>") {
        if let Some(ev_qual) = &ruletup.ev_qual {
            if (pretty_flags & PRETTYFLAG_INDENT) != 0 {
                append_str(mcx, &mut buf, "\n  ")?;
            }
            append_str(mcx, &mut buf, " WHERE ")?;

            // qual = stringToNode(ev_qual);
            let qual = read_seams::string_to_node::call(mcx, ev_qual.as_str())?;

            // The qual's Vars reference OLD/NEW; use the rtable of the first
            // action query (pushed into the SELECT for INSERT...SELECT).
            let action0 = &actions[0];
            let qual_query = rewrite_core::getInsertSelectQuery(action0)?;
            let mut qual_query = qual_query.clone_in(mcx)?;

            // AcquireRewriteLocks(query, false, false);
            ruleutils_seams::acquire_rewrite_locks::call(
                mcx,
                &mut qual_query,
                false,
                false,
            )?;

            // Build the deparse context for the qual.
            let varprefix = qual_query.rtable.len() != 1;
            let mut dpns = crate::DeparseNamespace::zeroed(mcx);
            let no_parent: [crate::DeparseNamespace<'mcx>; 0] = [];
            crate::set_deparse_for_query(mcx, &mut dpns, &qual_query, &no_parent)?;

            let mut namespaces: PgVec<'mcx, crate::DeparseNamespace<'mcx>> = PgVec::new_in(mcx);
            namespaces.try_reserve(1).map_err(|_| mcx.oom(0))?;
            namespaces.push(dpns);

            let mut context = crate::DeparseContext {
                buf,
                namespaces,
                resultDesc: None,
                targetList: PgVec::new_in(mcx),
                windowClause: PgVec::new_in(mcx),
                prettyFlags: pretty_flags,
                wrapColumn: WRAP_COLUMN_DEFAULT,
                indentLevel: PRETTYINDENT_STD,
                varprefix,
                colNamesVisible: true,
                inGroupBy: false,
                varInOrderBy: false,
                appendparents: None,
            };

            crate::expr_deparse::get_rule_expr(&qual, &mut context, false)?;
            buf = context.buf;
        }
    }

    append_str(mcx, &mut buf, " DO ")?;

    // The INSTEAD keyword (if so).
    if ruletup.is_instead {
        append_str(mcx, &mut buf, "INSTEAD ")?;
    }

    // Finally the rule's actions.
    if actions.len() > 1 {
        append_char(mcx, &mut buf, b'(')?;
        for action in actions.iter() {
            let q = action.clone_in(mcx)?;
            let no_parent: [crate::DeparseNamespace<'mcx>; 0] = [];
            buf = crate::query_deparse::get_query_def(
                mcx,
                &q,
                buf,
                &no_parent,
                clone_result_desc(mcx, &view_result_desc)?,
                true,
                pretty_flags,
                WRAP_COLUMN_DEFAULT,
                0,
            )?;
            if pretty_flags != 0 {
                append_str(mcx, &mut buf, ";\n")?;
            } else {
                append_str(mcx, &mut buf, "; ")?;
            }
        }
        append_str(mcx, &mut buf, ");")?;
    } else {
        let q = actions[0].clone_in(mcx)?;
        let no_parent: [crate::DeparseNamespace<'mcx>; 0] = [];
        buf = crate::query_deparse::get_query_def(
            mcx,
            &q,
            buf,
            &no_parent,
            view_result_desc,
            true,
            pretty_flags,
            WRAP_COLUMN_DEFAULT,
            0,
        )?;
        append_char(mcx, &mut buf, b';')?;
    }

    // table_close(ev_relation, AccessShareLock);
    ev_relation.close(AccessShareLock)?;

    Ok(Some(buf_to_string(mcx, buf)?))
}

/// `stringToNode(ev_action)` -> the single action `Query` (`make_viewdef`'s
/// `list_length(actions) != 1` guard). `Ok(None)` if the action list is empty
/// or not exactly one element, or not a `Query`.
fn single_action_query<'mcx>(
    mcx: Mcx<'mcx>,
    action_node: Option<PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<Option<::nodes::copy_query::Query<'mcx>>> {
    let mut actions = action_list_queries(mcx, action_node)?;
    if actions.len() != 1 {
        return Ok(None);
    }
    Ok(Some(actions.swap_remove(0)))
}

/// `(List *) stringToNode(ev_action)` -> the action queries, in order. An empty
/// / `<>` rendering yields an empty list (C's NIL).
fn action_list_queries<'mcx>(
    mcx: Mcx<'mcx>,
    action_node: Option<PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<Vec<::nodes::copy_query::Query<'mcx>>> {
    let mut out: Vec<::nodes::copy_query::Query<'mcx>> = Vec::new();
    let Some(action_node) = action_node else {
        return Ok(out);
    };
    let inner = PgBox::into_inner(action_node);
    match inner.node_tag() {
        ntag::T_List => {
            let elems = inner.into_list().unwrap();
            out.reserve(elems.len());
            for elem in elems {
                let elem_inner = PgBox::into_inner(elem);
                match elem_inner.node_tag() {
                    ntag::T_Query => out.push(elem_inner.into_query().unwrap()),
                    _ => {
                        return Err(PgError::error(format!(
                            "pg_rewrite ev_action element is {:?}, expected Query",
                            elem_inner.tag()
                        )));
                    }
                }
            }
            Ok(out)
        }
        ntag::T_Query => {
            // A bare single Query (defensive: C always stores a List, but the
            // reader may collapse a one-element list).
            out.push(inner.into_query().unwrap());
            Ok(out)
        }
        other => Err(PgError::error(format!(
            "pg_rewrite ev_action is {:?}, expected a List of Query",
            other
        ))),
    }
}

/// Clone an optional result-desc box (the multi-action `make_ruledef` arm reuses
/// `viewResultDesc` for every action; C aliases the pointer, the owned model
/// clones).
fn clone_result_desc<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
) -> PgResult<Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>> {
    match desc {
        None => Ok(None),
        Some(d) => Ok(Some(::mcx::alloc_in(mcx, d.clone_in(mcx)?)?)),
    }
}

/// `appendStringInfoString(buf, s)`.
fn append_str<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut stringinfo::StringInfo<'mcx>,
    s: &str,
) -> PgResult<()> {
    buf.data.try_reserve(s.len()).map_err(|_| mcx.oom(s.len()))?;
    buf.data.extend_from_slice(s.as_bytes());
    Ok(())
}

/// `appendStringInfoChar(buf, c)`.
fn append_char<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut stringinfo::StringInfo<'mcx>,
    c: u8,
) -> PgResult<()> {
    buf.data.try_reserve(1).map_err(|_| mcx.oom(1))?;
    buf.data.push(c);
    Ok(())
}

/// `buf.data` -> a `PgString` (the worker's `return buf.data`).
fn buf_to_string<'mcx>(
    mcx: Mcx<'mcx>,
    buf: stringinfo::StringInfo<'mcx>,
) -> PgResult<PgString<'mcx>> {
    let s = core::str::from_utf8(&buf.data)
        .map_err(|_| PgError::error("ruleutils: deparsed text is not valid UTF-8"))?;
    PgString::from_str_in(s, mcx)
}
