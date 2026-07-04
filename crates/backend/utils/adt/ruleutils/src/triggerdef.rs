//! pg_get_triggerdef family (ruleutils.c pg_get_triggerdef_worker).

use std::rc::Rc;

use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_nodes::primnodes::Alias;
use types_nodes::{Node, RangeTblEntry, RTEKind};
use types_rel::AccessShareLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_trigger::{
    TRIGGER_TYPE_AFTER, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT,
    TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_ROW, TRIGGER_TYPE_TIMING_MASK, TRIGGER_TYPE_TRUNCATE,
    TRIGGER_TYPE_UPDATE,
};
use types_tuple::{HeapTupleData, TupleDescData};

use crate::deparse::{get_rule_expr, simple_quote_literal, DeparseContext, PRETTYINDENT_STD};
use crate::query::{set_rtable_names, set_simple_column_names, DeparseNamespace};
use crate::ruledef::{req, text_attr};
use crate::viewdef::WRAP_COLUMN_DEFAULT;
use crate::{
    generate_function_name, generate_qualified_relation_name, generate_relation_name,
    get_pretty_flags, i16_array_at, name_at, quote_identifier,
};

const TRIGGER_RELATION_ID: Oid = 2620;
const TRIGGER_OID_INDEX_ID: Oid = 2702;

const ANUM_PG_TRIGGER_OID: i32 = 1;
const ANUM_PG_TRIGGER_TGRELID: i32 = 2;
const ANUM_PG_TRIGGER_TGNAME: i32 = 4;
const ANUM_PG_TRIGGER_TGFOID: i32 = 5;
const ANUM_PG_TRIGGER_TGTYPE: i32 = 6;
const ANUM_PG_TRIGGER_TGCONSTRRELID: i32 = 9;
const ANUM_PG_TRIGGER_TGCONSTRAINT: i32 = 11;
const ANUM_PG_TRIGGER_TGDEFERRABLE: i32 = 12;
const ANUM_PG_TRIGGER_TGINITDEFERRED: i32 = 13;
const ANUM_PG_TRIGGER_TGNARGS: i32 = 14;
const ANUM_PG_TRIGGER_TGATTR: i32 = 15;
const ANUM_PG_TRIGGER_TGARGS: i32 = 16;
const ANUM_PG_TRIGGER_TGQUAL: i32 = 17;
const ANUM_PG_TRIGGER_TGOLDTABLE: i32 = 18;
const ANUM_PG_TRIGGER_TGNEWTABLE: i32 = 19;

struct PgTriggerRow {
    tgname: String,
    tgrelid: Oid,
    tgfoid: Oid,
    tgtype: i16,
    tgconstrrelid: Oid,
    tgconstraint: Oid,
    tgdeferrable: bool,
    tginitdeferred: bool,
    tgnargs: i16,
    tgattr: Vec<i16>,
    tgargs: Vec<String>,
    tgqual: Option<String>,
    tgoldtable: Option<String>,
    tgnewtable: Option<String>,
}

fn opt(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> Option<Datum> {
    let mut isnull = false;
    // SAFETY: pg_trigger row read under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    if isnull {
        None
    } else {
        Some(d)
    }
}

fn bytea_strings(d: Datum, n: usize) -> Vec<String> {
    // SAFETY: non-null tgargs bytea datum addresses in-tuple bytes.
    let v = unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
    let bytes = v.data();
    let mut out = Vec::with_capacity(n);
    let mut p = 0usize;
    for _ in 0..n {
        let end = bytes[p..].iter().position(|&b| b == 0).map(|e| p + e).unwrap_or(bytes.len());
        out.push(
            core::str::from_utf8(&bytes[p..end]).expect("non-UTF-8 tgargs").to_owned(),
        );
        p = end + 1;
    }
    out
}

fn fetch_trigger(trigid: Oid) -> PgResult<Option<PgTriggerRow>> {
    let cx = MemoryContext::new("pg_get_triggerdef scan");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, TRIGGER_RELATION_ID, AccessShareLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = ANUM_PG_TRIGGER_OID as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::catalog::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)?;
    key.sk_argument = Datum::from_oid(trigid);
    let keys = [key];
    let mut scan = genam::systable_beginscan(
        smcx,
        &rel,
        TRIGGER_OID_INDEX_ID,
        relcache::criticalRelcachesBuilt(),
        None,
        &keys,
    )?;
    let mut row: Option<PgTriggerRow> = None;
    if let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let td = rel.descr();
        let tgnargs = req(td, tup, ANUM_PG_TRIGGER_TGNARGS).as_i16();
        row = Some(PgTriggerRow {
            tgname: name_at(req(td, tup, ANUM_PG_TRIGGER_TGNAME)),
            tgrelid: req(td, tup, ANUM_PG_TRIGGER_TGRELID).as_oid(),
            tgfoid: req(td, tup, ANUM_PG_TRIGGER_TGFOID).as_oid(),
            tgtype: req(td, tup, ANUM_PG_TRIGGER_TGTYPE).as_i16(),
            tgconstrrelid: req(td, tup, ANUM_PG_TRIGGER_TGCONSTRRELID).as_oid(),
            tgconstraint: req(td, tup, ANUM_PG_TRIGGER_TGCONSTRAINT).as_oid(),
            tgdeferrable: req(td, tup, ANUM_PG_TRIGGER_TGDEFERRABLE).as_bool(),
            tginitdeferred: req(td, tup, ANUM_PG_TRIGGER_TGINITDEFERRED).as_bool(),
            tgnargs,
            tgattr: i16_array_at(req(td, tup, ANUM_PG_TRIGGER_TGATTR)),
            tgargs: match opt(td, tup, ANUM_PG_TRIGGER_TGARGS) {
                Some(d) if tgnargs > 0 => bytea_strings(d, tgnargs as usize),
                _ => Vec::new(),
            },
            tgqual: match opt(td, tup, ANUM_PG_TRIGGER_TGQUAL) {
                Some(_) => Some(text_attr(td, tup, ANUM_PG_TRIGGER_TGQUAL)?),
                None => None,
            },
            tgoldtable: opt(td, tup, ANUM_PG_TRIGGER_TGOLDTABLE).map(name_at),
            tgnewtable: opt(td, tup, ANUM_PG_TRIGGER_TGNEWTABLE).map(name_at),
        });
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(row)
}

fn make_old_new_rte<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relkind: u8,
    aliasname: &'static str,
) -> PgResult<&'mcx RangeTblEntry<'mcx>> {
    let mut alias = Node::build::<Alias>(mcx)?;
    alias.aliasname = Some(aliasname);
    let alias = alias.seal().as_alias().unwrap();
    let mut rte = Node::build::<RangeTblEntry>(mcx)?;
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = relid;
    rte.relkind = relkind;
    rte.rellockmode = AccessShareLock as i32;
    rte.alias = Some(alias);
    rte.eref = Some(alias);
    rte.inFromCl = true;
    Ok(rte.seal().as_range_tbl_entry().unwrap())
}

pub fn pg_get_triggerdef_worker(
    mcx: Mcx<'_>,
    trigid: Oid,
    pretty: bool,
) -> PgResult<Option<String>> {
    let Some(trig) = fetch_trigger(trigid)? else {
        return Ok(None);
    };

    let mut buf = String::new();
    buf.push_str(&format!(
        "CREATE {}TRIGGER {} ",
        if trig.tgconstraint != 0 { "CONSTRAINT " } else { "" },
        quote_identifier(&trig.tgname)
    ));

    buf.push_str(match trig.tgtype & TRIGGER_TYPE_TIMING_MASK {
        TRIGGER_TYPE_BEFORE => "BEFORE",
        TRIGGER_TYPE_AFTER => "AFTER",
        TRIGGER_TYPE_INSTEAD => "INSTEAD OF",
        other => panic!("unexpected tgtype value: {other}"),
    });

    let mut findx = 0;
    if trig.tgtype & TRIGGER_TYPE_INSERT != 0 {
        buf.push_str(" INSERT");
        findx += 1;
    }
    if trig.tgtype & TRIGGER_TYPE_DELETE != 0 {
        buf.push_str(if findx > 0 { " OR DELETE" } else { " DELETE" });
        findx += 1;
    }
    if trig.tgtype & TRIGGER_TYPE_UPDATE != 0 {
        buf.push_str(if findx > 0 { " OR UPDATE" } else { " UPDATE" });
        findx += 1;
        if !trig.tgattr.is_empty() {
            buf.push_str(" OF ");
            for (i, attnum) in trig.tgattr.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                let attname = lsyscache::get_attname(mcx, trig.tgrelid, *attnum, false)?
                    .expect("get_attname missing_ok=false");
                buf.push_str(&quote_identifier(attname.as_str()));
            }
        }
    }
    if trig.tgtype & TRIGGER_TYPE_TRUNCATE != 0 {
        buf.push_str(if findx > 0 { " OR TRUNCATE" } else { " TRUNCATE" });
    }

    let relname = if pretty {
        generate_relation_name(mcx, trig.tgrelid)?
    } else {
        generate_qualified_relation_name(mcx, trig.tgrelid)?
    };
    buf.push_str(&format!(" ON {relname} "));

    if trig.tgconstraint != 0 {
        if trig.tgconstrrelid != 0 {
            buf.push_str(&format!("FROM {} ", generate_relation_name(mcx, trig.tgconstrrelid)?));
        }
        if !trig.tgdeferrable {
            buf.push_str("NOT ");
        }
        buf.push_str("DEFERRABLE INITIALLY ");
        buf.push_str(if trig.tginitdeferred { "DEFERRED " } else { "IMMEDIATE " });
    }

    if trig.tgoldtable.is_some() || trig.tgnewtable.is_some() {
        buf.push_str("REFERENCING ");
        if let Some(old) = &trig.tgoldtable {
            buf.push_str(&format!("OLD TABLE AS {} ", quote_identifier(old)));
        }
        if let Some(new) = &trig.tgnewtable {
            buf.push_str(&format!("NEW TABLE AS {} ", quote_identifier(new)));
        }
    }

    buf.push_str(if trig.tgtype & TRIGGER_TYPE_ROW != 0 {
        "FOR EACH ROW "
    } else {
        "FOR EACH STATEMENT "
    });

    if let Some(qual_str) = &trig.tgqual {
        buf.push_str("WHEN (");
        let qual = readfuncs::stringToNode(mcx, qual_str)?;
        let relkind = lsyscache::get_rel_relkind(trig.tgrelid)? as u8;
        let oldrte = make_old_new_rte(mcx, trig.tgrelid, relkind, "old")?;
        let newrte = make_old_new_rte(mcx, trig.tgrelid, relkind, "new")?;
        let mut dpns = DeparseNamespace::empty(vec![oldrte, newrte]);
        set_rtable_names(mcx, &mut dpns, &[], None)?;
        set_simple_column_names(mcx, &mut dpns)?;

        let mut ctx = DeparseContext::new(mcx, get_pretty_flags(pretty));
        ctx.namespaces.push(Rc::new(dpns));
        ctx.varprefix = true;
        ctx.wrap_column = WRAP_COLUMN_DEFAULT;
        ctx.indent_level = PRETTYINDENT_STD;
        get_rule_expr(qual, &mut ctx, false)?;
        buf.push_str(&ctx.buf);
        buf.push_str(") ");
    }

    buf.push_str(&format!(
        "EXECUTE FUNCTION {}(",
        generate_function_name(mcx, trig.tgfoid, &[], &[], false)?
    ));
    for (i, arg) in trig.tgargs.iter().enumerate() {
        if i > 0 {
            buf.push_str(", ");
        }
        simple_quote_literal(&mut buf, arg);
    }
    buf.push(')');
    Ok(Some(buf))
}
