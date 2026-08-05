//! pg_get_viewdef family (ruleutils.c pg_get_viewdef_worker + make_viewdef).

use std::rc::Rc;

use mcx::Mcx;
use types_core::{Oid, RELPERSISTENCE_PERMANENT};
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_rel::NoLock;

use crate::deparse::DeparseContext;
use crate::query;

pub(crate) const WRAP_COLUMN_DEFAULT: i32 = 0;

// C reads pg_rewrite through SPI keyed by (ev_class, "_RETURN"); the relcache
// rule cache carries no rulename, so the ON SELECT rule is selected by
// event == CMD_SELECT (an ON SELECT rule is unique and always "_RETURN").
pub fn pg_get_viewdef_worker(
    mcx: Mcx<'_>,
    viewoid: Oid,
    pretty_flags: i32,
    wrap_column: i32,
) -> PgResult<Option<String>> {
    let Some(rules) = relcache::rules::RelationGetRules(mcx, viewoid)? else {
        return Ok(None);
    };
    let Some(rule) = rules.rules.iter().find(|r| r.event == CmdType::CMD_SELECT as i32) else {
        return Ok(None);
    };
    if !rule.is_instead || rule.qual_src.is_some() {
        return Ok(None);
    }

    let actions = readfuncs::stringToNode(mcx, rule.action_src.as_str())?;
    let actions = actions.as_list().expect("ev_action is a List");
    if actions.len() != 1 {
        return Ok(None);
    }
    let query = actions.nth(0).as_query().expect("ev_action holds a Query");
    if query.commandType != CmdType::CMD_SELECT {
        return Ok(None);
    }

    let result_desc = Rc::new(view_attnames(viewoid)?);

    let mut ctx = DeparseContext::new(mcx, pretty_flags);
    ctx.wrap_column = wrap_column;
    query::get_query_def(query, &mut ctx, Some(result_desc), true)?;
    ctx.buf.push(';');
    Ok(Some(ctx.buf))
}

// RelationGetDescr(ev_relation) reduced to the attname-by-position slice
// get_target_list consults.
pub(crate) fn view_attnames(relid: Oid) -> PgResult<Vec<String>> {
    let natts = lsyscache::get_relnatts(relid)?;
    let mut out = Vec::with_capacity(natts.max(0) as usize);
    for attno in 1..=natts {
        let Some(att) = syscache_seams::lookup_pg_attribute_shape::call(relid, attno as i16)?
        else {
            return Err(crate::cache_lookup_failed("attribute", relid));
        };
        out.push(String::from_utf8_lossy(att.attname.name_str()).into_owned());
    }
    Ok(out)
}

// textToQualifiedNameList + makeRangeVarFromNameList + RangeVarGetRelid
// (NoLock, hard error) — the by-name pg_get_viewdef and
// pg_get_serial_sequence forms.
pub(crate) fn qualified_name_to_relid(mcx: Mcx<'_>, rawname: &str) -> PgResult<Oid> {
    let names = match varlena::split_identifier_string(
        mcx,
        rawname,
        b'.',
        mbutils::GetDatabaseEncoding(),
    )? {
        Some(names) if !names.is_empty() => names,
        _ => {
            return Err(types_error::PgError::error("invalid name syntax")
                .with_sqlstate(types_error::ERRCODE_INVALID_NAME)
                .into())
        }
    };
    let mut rv = rel_vocab::RangeVar {
        catalogname: None,
        schemaname: None,
        relname: "",
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    match names.as_slice() {
        [r] => rv.relname = r,
        [s, r] => {
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        [c, s, r] => {
            rv.catalogname = Some(c);
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        _ => {
            return Err(types_error::PgError::error(format!(
                "improper qualified name (too many dotted names): {rawname}"
            ))
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
            .into())
        }
    }
    catalog_namespace::RangeVarGetRelid(&rv, NoLock, false)
}

pub(crate) fn view_name_to_oid(mcx: Mcx<'_>, viewname: &str) -> PgResult<Oid> {
    qualified_name_to_relid(mcx, viewname)
}
