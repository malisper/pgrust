use mcx::{Mcx, MemoryContext, PgVec};
use relcache::schemapg::{REWRITE_RELATION_ID, REWRITE_REL_RULENAME_INDEX_ID};
use relcache_build_seams::PgRewriteRuleShape;
use types_core::Oid;
use types_error::PgResult;
use types_rel::AccessShareLock;
use types_tuple::{HeapTupleData, TupleDescData};

use crate::{oid_key, req};

const Anum_pg_rewrite_oid: i32 = 1;
const Anum_pg_rewrite_ev_class: i32 = 3;
const Anum_pg_rewrite_ev_type: i32 = 4;
const Anum_pg_rewrite_ev_enabled: i32 = 5;
const Anum_pg_rewrite_is_instead: i32 = 6;
const Anum_pg_rewrite_ev_qual: i32 = 7;
const Anum_pg_rewrite_ev_action: i32 = 8;

// Detoast a text attr datum (in-tuple varlena) into an mcx str; ev_action is
// routinely pglz-compressed inline for system views.
pub(crate) fn text_attr<'mcx>(
    mcx: Mcx<'mcx>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    attno: i32,
) -> PgResult<&'mcx str> {
    let d = req(td, tup, attno)?;
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
    let image = detoast::detoast_attr(mcx, raw)?;
    let bytes = mcx::slice_borrow_in(mcx, &image[datum::varlena::VARHDRSZ..])?;
    Ok(core::str::from_utf8(bytes).expect("pg_rewrite text attr is UTF-8"))
}

// RelationBuildRuleLock's pg_rewrite scan half (relcache.c): ev_class equality
// over RewriteRelRulenameIndexId, rulename order.
pub(crate) fn scan_pg_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    ev_class: Oid,
) -> PgResult<PgVec<'mcx, PgRewriteRuleShape<'mcx>>> {
    let cx = MemoryContext::new("ScanPgRewrite");
    let scan_mcx = cx.mcx();
    let rel = table::table_open(scan_mcx, REWRITE_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_rewrite_ev_class, ev_class)];
    let mut scan = genam::systable_beginscan(
        scan_mcx,
        &rel,
        REWRITE_REL_RULENAME_INDEX_ID,
        relcache::criticalRelcachesBuilt(),
        None,
        &keys,
    )?;
    let mut rows: PgVec<'mcx, PgRewriteRuleShape<'mcx>> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(scan_mcx, &mut scan)? {
        let td = rel.descr();
        rows.push(PgRewriteRuleShape {
            rule_id: req(td, tup, Anum_pg_rewrite_oid)?.as_oid(),
            ev_type: req(td, tup, Anum_pg_rewrite_ev_type)?.as_u8(),
            ev_enabled: req(td, tup, Anum_pg_rewrite_ev_enabled)?.as_u8(),
            is_instead: req(td, tup, Anum_pg_rewrite_is_instead)?.as_bool(),
            ev_qual: text_attr(mcx, td, tup, Anum_pg_rewrite_ev_qual)?,
            ev_action: text_attr(mcx, td, tup, Anum_pg_rewrite_ev_action)?,
        });
    }
    genam::systable_endscan(scan_mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(rows)
}
