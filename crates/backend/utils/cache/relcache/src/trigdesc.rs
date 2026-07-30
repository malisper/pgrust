use std::rc::Rc;

use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_trigger::TriggerDesc;

use crate::{cache_mcx, store};

#[track_caller]
#[cold]
#[inline(never)]
fn not_open(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "RelationGetTriggerDesc: relation {relid} not in relcache"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

// C hangs rd_trigdesc off the entry at RelationBuildDesc time; here it is
// built on first ask (rd_indexlist precedent) and the Rc clone replaces C's
// per-query CopyTriggerDesc.
pub fn RelationGetTriggerDesc(relid: Oid) -> PgResult<Option<Rc<TriggerDesc<'static>>>> {
    let rel = store::RelationIdGetRelation(relid)?.ok_or_else(|| not_open(relid))?;
    if !rel.rd_hastriggers {
        return Ok(None);
    }
    if let Some(cached) = rel.rd_trigdesc.borrow().as_ref() {
        return Ok(Some(Rc::clone(cached)));
    }
    // The scan re-enters the relcache; no borrow held across it.
    let built = relcache_build_seams::build_trigger_desc::call(cache_mcx(), relid)?;
    let Some(desc) = built else {
        return Ok(None);
    };
    let rc = Rc::new(desc);
    *rel.rd_trigdesc.borrow_mut() = Some(Rc::clone(&rc));
    Ok(Some(rc))
}
