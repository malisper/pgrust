//! Installation of this unit's six inward seams.
//!
//! The seam contracts (`backend-commands-publicationcmds-seams`) are `Mcx`-free
//! (they are reached from `backend-catalog-dependency`'s per-class drop handlers
//! and from `backend-catalog-pg-shdepend`'s REASSIGN OWNED, neither of which has
//! an `Mcx` in scope). The ported implementations need an `Mcx` for catalog
//! reads/allocation, and `mcx` has no ambient current context — so each
//! installer wrapper spins up a fresh `MemoryContext` and runs the
//! `Mcx`-taking implementation in it (the established bridging idiom, cf.
//! `backend-commands-foreigncmds::init_seams` / `backend-commands-matview`).

use backend_commands_publicationcmds_seams as s;

pub fn init_seams() {
    s::alter_publication_owner_oid::set(|pubid, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterPublicationOwner_oid");
        crate::AlterPublicationOwner_oid(ctx.mcx(), pubid, new_owner_id)
    });

    s::RemovePublicationById::set(|pubid| {
        let ctx = mcx::MemoryContext::new("RemovePublicationById");
        crate::RemovePublicationById(ctx.mcx(), pubid)
    });

    s::RemovePublicationRelById::set(|proid| {
        let ctx = mcx::MemoryContext::new("RemovePublicationRelById");
        crate::RemovePublicationRelById(ctx.mcx(), proid)
    });

    s::RemovePublicationSchemaById::set(|psoid| {
        let ctx = mcx::MemoryContext::new("RemovePublicationSchemaById");
        crate::RemovePublicationSchemaById(ctx.mcx(), psoid)
    });

    s::AlterPublicationOwner::set(|name, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterPublicationOwner");
        crate::AlterPublicationOwner(ctx.mcx(), name, new_owner_id)
    });

    s::InvalidatePubRelSyncCache::set(|pubid, puballtables| {
        let ctx = mcx::MemoryContext::new("InvalidatePubRelSyncCache");
        crate::InvalidatePubRelSyncCache(ctx.mcx(), pubid, puballtables)
    });

    // The two REPLICA IDENTITY validity checks `RelationBuildPublicationDesc`
    // runs per publishing publication. These carry an `Mcx` and an `'mcx`-bound
    // `&Relation` directly, so they install without the fresh-context bridge.
    s::pub_rf_contains_invalid_column::set(crate::pub_rf_contains_invalid_column);
    s::pub_contains_invalid_column::set(crate::pub_contains_invalid_column);
}
