//! pg_get_viewdef family (ruleutils.c pg_get_viewdef_worker + make_viewdef).

use std::rc::Rc;

use mcx::Mcx;
use types_core::Oid;
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
    crate::check_pg_rewrite_select(
        "SELECT * FROM pg_catalog.pg_rewrite WHERE ev_class = $1 AND rulename = $2",
    )?;
    let Some(rules) = relcache::rules::RelationGetRules(mcx, viewoid)? else {
        return Ok(None);
    };
    let Some(rule) = rules.rules.iter().find(|r| r.event == CmdType::CMD_SELECT as i32) else {
        return Ok(None);
    };
    if !rule.is_instead || rule.has_qual() {
        return Ok(None);
    }

    // C's pg_get_viewdef_worker stringToNode's a fresh tree from pg_rewrite
    // (not rd_rules), and get_query_def's AcquireRewriteLocks scribbles on
    // it (dropped-column fix-up of JOIN RTEs): deparse a private copy.
    let actions = rule.copy_actions(mcx)?;
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
    Ok(Some(ctx.buf.into_inner()))
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
// pg_get_serial_sequence forms; rawname is the detoasted text.
// makeRangeVarFromNameList + RangeVarGetRelid; the relname part comes back
// for messages that print tablerv->relname.
pub(crate) fn qualified_name_to_relid_relname<'mcx>(
    mcx: Mcx<'mcx>,
    rawname: &[u8],
) -> PgResult<(Oid, mcx::PgVec<'mcx, u8>)> {
    let mut names = varlena::textToQualifiedNameList(mcx, rawname)?;
    let parts: Vec<&[u8]> = names.iter().map(|n| n.as_slice()).collect();
    let relid = catalog_namespace::RangeVarGetRelidFromNameBytes(&parts, NoLock, false)?;
    let relname = names.pop().expect("a resolved relation name has a last part");
    Ok((relid, relname))
}

pub(crate) fn qualified_name_to_relid(mcx: Mcx<'_>, rawname: &[u8]) -> PgResult<Oid> {
    Ok(qualified_name_to_relid_relname(mcx, rawname)?.0)
}

pub(crate) fn view_name_to_oid(mcx: Mcx<'_>, viewname: &[u8]) -> PgResult<Oid> {
    qualified_name_to_relid(mcx, viewname)
}
