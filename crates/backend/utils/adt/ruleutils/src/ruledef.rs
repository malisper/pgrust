//! pg_get_ruledef family (ruleutils.c pg_get_ruledef_worker + make_ruledef).
//! C reads pg_rewrite through SPI; this scans the oid index directly.

use std::rc::Rc;

use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_rel::AccessShareLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

use crate::deparse::{get_rule_expr, DeparseContext, PRETTYINDENT_STD};
use crate::query::{get_query_def, set_deparse_for_query};
use crate::viewdef::{view_attnames, WRAP_COLUMN_DEFAULT};
use crate::{
    generate_qualified_relation_name, generate_relation_name, quote_identifier, PRETTYFLAG_INDENT,
    PRETTYFLAG_SCHEMA,
};

const REWRITE_RELATION_ID: Oid = 2618;
const REWRITE_OID_INDEX_ID: Oid = 2692;

const ANUM_PG_REWRITE_OID: i32 = 1;
const ANUM_PG_REWRITE_RULENAME: i32 = 2;
const ANUM_PG_REWRITE_EV_CLASS: i32 = 3;
const ANUM_PG_REWRITE_EV_TYPE: i32 = 4;
const ANUM_PG_REWRITE_IS_INSTEAD: i32 = 6;
const ANUM_PG_REWRITE_EV_QUAL: i32 = 7;
const ANUM_PG_REWRITE_EV_ACTION: i32 = 8;

struct PgRewriteRow {
    rulename: String,
    ev_class: Oid,
    ev_type: u8,
    is_instead: bool,
    ev_qual: String,
    ev_action: String,
}

pub(crate) fn req(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> Datum {
    let mut isnull = false;
    // SAFETY: pg_rewrite row read under its relation's descriptor; every
    // attno here is NOT NULL in pg_rewrite.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    assert!(!isnull, "unexpected null in pg_rewrite column {attno}");
    d
}

// ev_qual/ev_action are routinely pglz-compressed inline.
pub(crate) fn text_attr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> PgResult<String> {
    let d = req(td, tup, attno);
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum addresses in-tuple bytes; length is
    // taken from its own header before slicing.
    let raw = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            detoast::varsize_any(core::slice::from_raw_parts(p, 2))
        } else if b0 & 0x01 != 0 {
            ((b0 >> 1) & 0x7F) as usize
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    let scratch = MemoryContext::new("pg_rewrite text attr");
    let image = detoast::detoast_attr(scratch.mcx(), raw)?;
    Ok(String::from_utf8(image[datum::varlena::VARHDRSZ..].to_vec())
        .expect("pg_rewrite text attr is UTF-8"))
}

fn fetch_rule(ruleoid: Oid) -> PgResult<Option<PgRewriteRow>> {
    let cx = MemoryContext::new("pg_get_ruledef scan");
    let scan_mcx = cx.mcx();
    let rel = table::table_open(scan_mcx, REWRITE_RELATION_ID, AccessShareLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = ANUM_PG_REWRITE_OID as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::catalog::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)?;
    key.sk_argument = Datum::from_oid(ruleoid);
    let keys = [key];
    let mut scan = genam::systable_beginscan(
        scan_mcx,
        &rel,
        REWRITE_OID_INDEX_ID,
        relcache::criticalRelcachesBuilt(),
        None,
        &keys,
    )?;
    let mut row: Option<PgRewriteRow> = None;
    if let Some(tup) = genam::systable_getnext(scan_mcx, &mut scan)? {
        let td = rel.descr();
        let name = req(td, tup, ANUM_PG_REWRITE_RULENAME);
        // SAFETY: rulename NameData column inside the tuple image.
        let name = unsafe { *(name.as_usize() as *const NameData) };
        row = Some(PgRewriteRow {
            rulename: String::from_utf8_lossy(name.name_str()).into_owned(),
            ev_class: req(td, tup, ANUM_PG_REWRITE_EV_CLASS).as_oid(),
            ev_type: req(td, tup, ANUM_PG_REWRITE_EV_TYPE).as_u8(),
            is_instead: req(td, tup, ANUM_PG_REWRITE_IS_INSTEAD).as_bool(),
            ev_qual: text_attr(td, tup, ANUM_PG_REWRITE_EV_QUAL)?,
            ev_action: text_attr(td, tup, ANUM_PG_REWRITE_EV_ACTION)?,
        });
    }
    genam::systable_endscan(scan_mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(row)
}

pub fn pg_get_ruledef_worker(
    mcx: Mcx<'_>,
    ruleoid: Oid,
    pretty_flags: i32,
) -> PgResult<Option<String>> {
    let Some(rule) = fetch_rule(ruleoid)? else {
        return Ok(None);
    };
    Ok(Some(make_ruledef(mcx, &rule, pretty_flags)?))
}

fn make_ruledef(mcx: Mcx<'_>, rule: &PgRewriteRow, pretty_flags: i32) -> PgResult<String> {
    let actions_node = readfuncs::stringToNode(mcx, &rule.ev_action)?;
    let actions = actions_node.as_list().expect("ev_action is a List");
    assert!(!actions.is_nil(), "invalid empty ev_action list");

    let mut ctx = DeparseContext::new(mcx, pretty_flags);
    ctx.wrap_column = WRAP_COLUMN_DEFAULT;
    ctx.buf
        .push_str(&format!("CREATE RULE {} AS", quote_identifier(&rule.rulename)));
    ctx.buf.push_str(if pretty_flags & PRETTYFLAG_INDENT != 0 {
        "\n    ON "
    } else {
        " ON "
    });

    let mut view_result_desc: Option<Rc<Vec<String>>> = None;
    match rule.ev_type {
        b'1' => {
            ctx.buf.push_str("SELECT");
            view_result_desc = Some(Rc::new(view_attnames(rule.ev_class)?));
        }
        b'2' => ctx.buf.push_str("UPDATE"),
        b'3' => ctx.buf.push_str("INSERT"),
        b'4' => ctx.buf.push_str("DELETE"),
        other => panic!(
            "rule \"{}\" has unsupported event type {other}",
            rule.rulename
        ),
    }

    let relname = if pretty_flags & PRETTYFLAG_SCHEMA != 0 {
        generate_relation_name(mcx, rule.ev_class)?
    } else {
        generate_qualified_relation_name(mcx, rule.ev_class)?
    };
    ctx.buf.push_str(&format!(" TO {relname}"));

    if rule.ev_qual != "<>" {
        if pretty_flags & PRETTYFLAG_INDENT != 0 {
            ctx.buf.push_str("\n  ");
        }
        ctx.buf.push_str(" WHERE ");
        let qual = readfuncs::stringToNode(mcx, &rule.ev_qual)?;
        let first = actions.nth(0).as_query().expect("ev_action holds Queries");
        let query = match rewrite_manip::getInsertSelectQuery_parts(first)? {
            Some((_, sub)) => sub,
            None => first,
        };
        // C AcquireRewriteLocks here; names read the live catalogs unlocked
        // (get_query_def precedent).
        let dpns = set_deparse_for_query(mcx, query, &[])?;
        ctx.varprefix = query.rtable.len() != 1;
        ctx.indent_level = PRETTYINDENT_STD;
        ctx.namespaces.push(Rc::new(dpns));
        get_rule_expr(qual, &mut ctx, false)?;
        ctx.namespaces.clear();
        ctx.varprefix = false;
        ctx.indent_level = 0;
    }

    ctx.buf.push_str(" DO ");
    if rule.is_instead {
        ctx.buf.push_str("INSTEAD ");
    }

    if actions.len() > 1 {
        ctx.buf.push('(');
        for action in actions.iter() {
            let query = action.as_query().expect("ev_action holds Queries");
            get_query_def(query, &mut ctx, view_result_desc.clone(), true)?;
            ctx.buf.push_str(if pretty_flags != 0 { ";\n" } else { "; " });
        }
        ctx.buf.push_str(");");
    } else {
        let query = actions.nth(0).as_query().expect("ev_action holds Queries");
        get_query_def(query, &mut ctx, view_result_desc, true)?;
        ctx.buf.push(';');
    }
    Ok(ctx.buf)
}
