//! Install every seam declared in `backend-utils-mmgr-portalmem-seams` to this
//! crate's real functions. Called once from `seams-init`'s `init_all()`.
//!
//! The inward seams xact (the cyclic caller) invokes drop the parent
//! `ResourceOwner` arguments — resource owners dissolve into RAII owner values
//! (docs/query-lifecycle-raii.md), so the parent-owner reparenting in
//! `AtSubCommit`/`AtSubAbort` threads a default (NULL) owner; the reparent seam
//! is a no-op until the resowner side lands.
//!
//! The deep-copy-into-portal-context seams (`portal_define_query_select`,
//! `portal_define_query_list`, `copy_param_list_into_portal`,
//! `copy_tup_desc_into_hold_context`) intern foreign objects into the portal's
//! own owned `MemoryContext` arenas (`portalContext`/`holdContext`). The arena
//! lifetime crosses as a `'static` marker on the portal fields; the copied
//! value is real `Global`-heap memory owned by its inner `PgBox`/`PgVec`, freed
//! by its own `Drop`. `PortalData::drop` and `PortalDrop` release those payload
//! fields before the contexts they deallocate through, so the marker is sound
//! (the same idiom `portal_set_tup_desc`/`set_result_tup_desc_with` already use
//! for `tupDesc`). No `'static` global arena or token registry is involved.

use types_core::SubTransactionId;
use types_error::PgResult;
use types_portal::ResourceOwner;

use backend_utils_mmgr_portalmem_seams as seams;

pub fn init_seams() {
    // xact-facing lifecycle seams.
    seams::pre_commit_portals::set(crate::PreCommit_Portals);
    seams::at_abort_portals::set(crate::AtAbort_Portals);
    seams::at_cleanup_portals::set(crate::AtCleanup_Portals);
    seams::at_subcommit_portals::set(at_subcommit_portals);
    seams::at_subabort_portals::set(at_subabort_portals);
    seams::at_subcleanup_portals::set(crate::AtSubCleanup_Portals);

    // portalcmds-facing portal-operation seams.
    seams::create_portal::set(|name, allow_dup, dup_silent| {
        crate::CreatePortal(name, allow_dup, dup_silent)
    });
    seams::get_portal_by_name::set(|name| Ok(crate::GetPortalByName(Some(name))));
    seams::portal_hash_table_delete_all::set(crate::PortalHashTableDeleteAll);
    seams::portal_drop::set(|portal, is_top_commit| crate::PortalDrop(portal, is_top_commit));
    seams::mark_portal_active::set(|portal| crate::MarkPortalActive(portal));
    seams::mark_portal_failed::set(|portal| crate::MarkPortalFailed(portal));
    seams::memory_context_delete_children::set(|portal| {
        crate::memory_context_delete_children(portal)
    });
    seams::with_portal_globals::set(|portal, f| crate::with_portal_globals(portal, f));

    // Deep-copy-into-portal-context seams (intern foreign objects into the
    // portal's own owned arenas; sound `'static` marker, see module docs).
    seams::portal_define_query_select::set(|portal, source_text, plan| {
        crate::portal_define_query_select(portal, source_text, plan)
    });
    seams::portal_define_query_list::set(
        |portal, prep_stmt_name, source_text, command_tag, stmts, cplan| {
            crate::portal_define_query_list(
                portal,
                prep_stmt_name,
                source_text,
                command_tag,
                stmts,
                cplan,
            )
        },
    );
    seams::copy_param_list_into_portal::set(|portal, params| {
        crate::copy_param_list_into_portal(portal, params)
    });
    seams::copy_tup_desc_into_hold_context::set(|portal| {
        crate::copy_tup_desc_into_hold_context(portal)
    });

    // Pure-wiring install (assemble/seam-wiring-guard): owner body matches.
    seams::enable_portal_manager::set(crate::EnablePortalManager);
}

/// `AtSubCommit_Portals(mySubid, parentSubid, parentLevel, parentXactOwner)`
/// with the owner argument dissolved.
fn at_subcommit_portals(
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
    parent_level: i32,
) -> PgResult<()> {
    crate::AtSubCommit_Portals(my_subid, parent_subid, parent_level, ResourceOwner::default())
}

/// `AtSubAbort_Portals(mySubid, parentSubid, myXactOwner, parentXactOwner)`
/// with the owner arguments dissolved.
fn at_subabort_portals(my_subid: SubTransactionId, parent_subid: SubTransactionId) -> PgResult<()> {
    crate::AtSubAbort_Portals(
        my_subid,
        parent_subid,
        ResourceOwner::default(),
        ResourceOwner::default(),
    )
}
