use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use relcache::schemapg::{REWRITE_REL_RULENAME_INDEX_ID, REWRITE_RELATION_ID};
use relcache_build_seams::PgRewriteRuleShape;
use types_core::Oid;
use types_error::PgResult;
use types_rel::AccessShareLock;

use crate::{getattr, oid_key, req};

const Anum_pg_rewrite_oid: i32 = 1;
const Anum_pg_rewrite_ev_class: i32 = 3;
const Anum_pg_rewrite_ev_type: i32 = 4;
const Anum_pg_rewrite_ev_enabled: i32 = 5;
const Anum_pg_rewrite_is_instead: i32 = 6;
const Anum_pg_rewrite_ev_qual: i32 = 7;
const Anum_pg_rewrite_ev_action: i32 = 8;

// RelationBuildRuleLock's pg_rewrite scan (relcache.c), rule-name order.
pub fn scan_pg_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    ev_class: Oid,
) -> PgResult<PgVec<'mcx, PgRewriteRuleShape<'mcx>>> {
    let mut rules: PgVec<'mcx, PgRewriteRuleShape<'mcx>> = PgVec::new_in(mcx);
    let rel = table::table_open(mcx, REWRITE_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_rewrite_ev_class, ev_class)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        REWRITE_REL_RULENAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let shape = PgRewriteRuleShape {
            rule_id: req(rel.descr(), tup, Anum_pg_rewrite_oid)?.as_oid(),
            ev_type: req(rel.descr(), tup, Anum_pg_rewrite_ev_type)?.as_i8() as u8,
            ev_enabled: req(rel.descr(), tup, Anum_pg_rewrite_ev_enabled)?.as_i8() as u8,
            is_instead: req(rel.descr(), tup, Anum_pg_rewrite_is_instead)?.as_bool(),
            ev_qual: node_text(mcx, req(rel.descr(), tup, Anum_pg_rewrite_ev_qual)?)?,
            ev_action: node_text(mcx, req(rel.descr(), tup, Anum_pg_rewrite_ev_action)?)?,
        };
        rules.push(shape);
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(rules)
}

fn node_text<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<&'mcx str> {
    let p = d.as_usize() as *const u8;
    // SAFETY: d comes off a not-null text column: a live varlena image
    // readable through its varsize_any extent.
    let image = unsafe { std::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let s = core::str::from_utf8(payload.as_bytes())
        .unwrap_or_else(|_| panic!("non-UTF-8 pg_rewrite node tree text"));
    let bytes = PgString::from_str_in(s, mcx)?.into_bytes().leak();
    // SAFETY: bytes are a whole PgString image, UTF-8 by construction.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}
