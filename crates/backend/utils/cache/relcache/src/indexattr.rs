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
    // C's restart loop (relcache.c 5344-5510): concurrent index DDL can change
    // the set mid-build; recheck list + pk/replident stability before caching.
    let bm = loop {
        // No state borrow across these: they re-enter the relcache.
        let index_oids = crate::indexlist::RelationGetIndexList(cmcx, relid)?;
        let rel = store::RelationIdGetRelation(relid)?.ok_or_else(|| index_missing(relid))?;
        let (pk_index, replident_index) = pk_replident(&rel);
        let attempt = build_bitmaps(cmcx, &index_oids, pk_index, replident_index)?;
        let new_oids = crate::indexlist::RelationGetIndexList(cmcx, relid)?;
        let rel = store::RelationIdGetRelation(relid)?.ok_or_else(|| index_missing(relid))?;
        let (pk2, ri2) = pk_replident(&rel);
        if new_oids[..] == index_oids[..] && pk2 == pk_index && ri2 == replident_index {
            break attempt;
        }
    };
    let built = Rc::new(bm);
    with_state(|st| st.indexattr_cache.insert(relid, Rc::clone(&built)));
    Ok(built)
}

fn pk_replident(rel: &Rc<types_rel::RelationData<'static>>) -> (Oid, Oid) {
    rel.rd_indexlist
        .borrow()
        .as_ref()
        .map(|l| (l.pkindex, l.replidindex))
        .unwrap_or((types_core::InvalidOid, types_core::InvalidOid))
}

fn build_bitmaps(
    cmcx: mcx::Mcx<'static>,
    index_oids: &[Oid],
    pk_index: Oid,
    replident_index: Oid,
) -> PgResult<IndexAttrBitmaps> {
    let mut bm = IndexAttrBitmaps {
        hot_blocking: PgVec::new_in(cmcx),
        summarized: PgVec::new_in(cmcx),
        key: PgVec::new_in(cmcx),
        pk: PgVec::new_in(cmcx),
        identity: PgVec::new_in(cmcx),
    };
    for &index_oid in index_oids.iter() {
        let irel = store::RelationIdGetRelation(index_oid)?
            .ok_or_else(|| index_missing(index_oid))?;
        let form = irel.rd_index.as_ref().ok_or_else(|| index_missing(index_oid))?;
        let summarizing = irel.rd_rel.relam == BRIN_AM_OID;
        let is_key =
            form.indisunique && form.indexprs_src.is_none() && form.indpred_src.is_none();
        let is_pk = index_oid == pk_index;
        let is_id_key = index_oid == replident_index;
        for (i, &attnum) in form.indkey.iter().enumerate() {
            if attnum == 0 {
                continue;
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
                if is_pk {
                    add(&mut bm.pk, attnum);
                }
                if is_id_key {
                    add(&mut bm.identity, attnum);
                }
            }
        }
        // pull_varattnos over the untransformed stringToNode trees (relcache.c
        // 5392-5398): folding could drop a Var from the HOT-blocking set.
        for src in [form.indexprs_src.as_ref(), form.indpred_src.as_ref()]
            .into_iter()
            .flatten()
        {
            let target = if summarizing { &mut bm.summarized } else { &mut bm.hot_blocking };
            pull_expr_attrs(src.as_str(), target)?;
        }
    }
    Ok(bm)
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| {
        st.indexattr_cache.remove(&relid);
    });
}

#[track_caller]
#[cold]
#[inline(never)]
fn index_missing(index_oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not open index {index_oid} for attr bitmap"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn pull_expr_attrs(src: &str, out: &mut PgVec<'static, i16>) -> PgResult<()> {
    struct W<'a> {
        out: &'a mut PgVec<'static, i16>,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_> {
        fn visit(&mut self, node: types_nodes::Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                assert!(
                    v.varno == 1 && v.varlevelsup == 0 && v.varattno > 0,
                    "pull_varattnos (relcache index lane): unexpected Var shape"
                );
                add(self.out, v.varattno);
                return Ok(false);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let cx = mcx::MemoryContext::new("IndexAttrExprPull");
    let smcx = cx.mcx();
    let node = readfuncs::stringToNode(smcx, src)?;
    nodes_core::NodeWalker::visit(&mut W { out }, node)?;
    Ok(())
}
