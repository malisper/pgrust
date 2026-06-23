//! Seam installation for the heap page-pruning unit.
//!
//! Installs both this crate's inward seams (declared in
//! `backend-access-heap-pruneheap-seams`) and the prune/freeze + WAL-emit seams
//! the merged VACUUM driver reaches across the dependency cycle (declared in
//! `backend-access-heap-vacuumlazy-seams`, owned here because pruneheap.c owns
//! `heap_page_prune_and_freeze` / `log_heap_prune_and_freeze`).

extern crate alloc;

use ::mcx::MemoryContext;
use ::types_error::PgError;
use ::types_vacuum::vacuumlazy::{PruneAndFreezeArgs, PruneAndFreezeOut};

use pruneheap_seams as pruneheap_seam;
use vacuumlazy_seams as vacuumlazy_seam;
use relcache_seams as relcache_seam;

/// Install every seam this crate owns.
pub fn init_seams() {
    // ---- inward seams (pruneheap-seams) -------------------------------------
    pruneheap_seam::heap_page_prune_opt::set(|mcx, relation, buffer| {
        crate::heap_page_prune_opt(mcx, relation, buffer)
    });

    pruneheap_seam::heap_page_prune_execute::set(
        |buffer, lp_truncate_only, redirected, nowdead, nowunused| {
            crate::heap_page_prune_execute(buffer, lp_truncate_only, redirected, nowdead, nowunused)
        },
    );

    pruneheap_seam::heap_get_root_tuples::set(|mcx, buffer| crate::heap_get_root_tuples(mcx, buffer));

    // ---- prune/freeze + WAL-emit seams owned here (vacuumlazy-seams) --------
    vacuumlazy_seam::heap_page_prune_and_freeze::set(prune_and_freeze_seam);

    vacuumlazy_seam::log_heap_prune_and_freeze::set(
        |relation, buffer, conflict_xid, cleanup_lock, reason, mut frozen, redirected, dead, unused| {
            // The driver passes the relation by OID; resolve the relcache entry
            // for the logical-decoding / catalog-rel flag in the WAL record.
            let ctx = MemoryContext::new("log_heap_prune_and_freeze");
            let mcx = ctx.mcx();
            let rel = relcache_seam::relation_id_get_relation::call(mcx, relation)?
                .ok_or_else(|| PgError::error("relation no longer exists"))?;
            let r = crate::log_heap_prune_and_freeze(
                &rel,
                buffer,
                conflict_xid,
                cleanup_lock,
                reason,
                &mut frozen,
                &redirected,
                &dead,
                &unused,
            );
            let _ = relcache_seam::relation_close::call(relation);
            r
        },
    );
}

/// Bridge the `PruneAndFreezeArgs`/`PruneAndFreezeOut` value contract (the
/// merged driver passes the relation by OID) onto the owned-reference engine.
fn prune_and_freeze_seam(args: PruneAndFreezeArgs) -> ::types_error::PgResult<PruneAndFreezeOut> {
    let ctx = MemoryContext::new("heap_page_prune_and_freeze");
    let mcx = ctx.mcx();

    let rel = relcache_seam::relation_id_get_relation::call(mcx, args.relation)?
        .ok_or_else(|| PgError::error("relation no longer exists"))?;

    let mut off_loc = args.off_loc_in;
    let mut new_relfrozen_xid = args.new_relfrozen_xid_in;
    let mut new_relmin_mxid = args.new_relmin_mxid_in;

    let freeze = (args.options & crate::HEAP_PAGE_PRUNE_FREEZE) != 0;

    let presult = if freeze {
        crate::heap_page_prune_and_freeze(
            mcx,
            &rel,
            args.buffer,
            args.vistest,
            args.options,
            Some(&args.cutoffs),
            args.reason,
            &mut off_loc,
            Some(&mut new_relfrozen_xid),
            Some(&mut new_relmin_mxid),
        )
    } else {
        crate::heap_page_prune_and_freeze(
            mcx,
            &rel,
            args.buffer,
            args.vistest,
            args.options,
            Some(&args.cutoffs),
            args.reason,
            &mut off_loc,
            None,
            None,
        )
    };

    let presult = match presult {
        Ok(p) => p,
        Err(e) => {
            let _ = relcache_seam::relation_close::call(args.relation);
            return Err(e);
        }
    };

    let _ = relcache_seam::relation_close::call(args.relation);

    Ok(PruneAndFreezeOut {
        presult,
        new_relfrozen_xid,
        new_relmin_mxid,
        off_loc,
    })
}
