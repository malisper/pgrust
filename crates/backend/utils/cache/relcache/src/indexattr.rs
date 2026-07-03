use std::rc::Rc;

use mcx::PgVec;
use relcache_seams::IndexAttrBitmaps;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};

use crate::{cache_mcx, store, with_state};

const BRIN_AM_OID: Oid = 3580;

fn add(v: &mut PgVec<'static, i16>, attnum: i16) {
    match v.binary_search(&attnum) {
        Ok(_) => {}
        Err(pos) => v.insert(pos, attnum),
    }
}

// RelationGetIndexAttrBitmap (relcache.c), all kinds in one pass; the rule-5
// cache is a relid-keyed side table (rules.rs precedent — the trimmed
// RelationData has no rd_attrsvalid field).
pub fn RelationGetIndexAttrBitmap(relid: Oid) -> PgResult<Rc<IndexAttrBitmaps>> {
    if let Some(hit) = with_state(|st| st.indexattr_cache.get(&relid).cloned()) {
        return Ok(hit);
    }
    let cmcx = cache_mcx();
    // No state borrow across these: they re-enter the relcache.
    let index_oids = crate::indexlist::RelationGetIndexList(cmcx, relid)?;
    let rel = store::RelationIdGetRelation(relid)?.ok_or_else(|| index_missing(relid))?;
    let replident_index = rel
        .rd_indexlist
        .borrow()
        .as_ref()
        .map(|l| l.replidindex)
        .unwrap_or(types_core::InvalidOid);
    let mut bm = IndexAttrBitmaps {
        hot_blocking: PgVec::new_in(cmcx),
        summarized: PgVec::new_in(cmcx),
        key: PgVec::new_in(cmcx),
        identity: PgVec::new_in(cmcx),
    };
    for &index_oid in index_oids.iter() {
        let irel = store::RelationIdGetRelation(index_oid)?
            .ok_or_else(|| index_missing(index_oid))?;
        let form = irel.rd_index.as_ref().ok_or_else(|| index_missing(index_oid))?;
        if form.has_indpred {
            panic!(
                "RelationGetIndexAttrBitmap (relcache.c): partial index {index_oid} \
                 (indpred var pull unported)"
            );
        }
        let summarizing = irel.rd_rel.relam == BRIN_AM_OID;
        let is_key = form.indisunique && form.indimmediate;
        let is_id_key = index_oid == replident_index;
        for (i, &attnum) in form.indkey.iter().enumerate() {
            if attnum == 0 {
                panic!(
                    "RelationGetIndexAttrBitmap (relcache.c): expression index \
                     {index_oid} (indexprs var pull unported)"
                );
            }
            assert!(attnum > 0, "system-column index key");
            if summarizing {
                add(&mut bm.summarized, attnum);
            } else {
                add(&mut bm.hot_blocking, attnum);
            }
            if i < form.indnkeyatts as usize {
                if is_key {
                    add(&mut bm.key, attnum);
                }
                if is_id_key {
                    add(&mut bm.identity, attnum);
                }
            }
        }
    }
    let built = Rc::new(bm);
    with_state(|st| st.indexattr_cache.insert(relid, Rc::clone(&built)));
    Ok(built)
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| {
        st.indexattr_cache.remove(&relid);
    });
}

#[cold]
#[inline(never)]
fn index_missing(index_oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not open index {index_oid} for attr bitmap"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}
