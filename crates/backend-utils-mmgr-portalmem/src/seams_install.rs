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
    seams::create_new_portal::set(crate::CreateNewPortal);
    seams::portal_set_visible::set(|portal, value| {
        portal.borrow_mut().visible = value;
        Ok(())
    });
    seams::get_portal_by_name::set(|name| Ok(crate::GetPortalByName(Some(name))));
    // `execCurrentOf`'s GetPortalByName-lending seam: hand the named cursor's
    // live EState/PlanState to the callback for its duration.
    seams::with_running_cursor::set(|name, f| crate::with_running_cursor(name, f));
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

    // `MemoryContextInit()` (utils/mmgr/mcxt.c). In stock PG this creates the
    // process-global `TopMemoryContext`/`ErrorContext` very early in `main()`.
    // This repo's `mcx` model has NO ambient/global context (docs/mctx-design.md:
    // "deliberately no ambient current context"): every context is an owned
    // value threaded through `Mcx<'mcx>`, and the top-level context the boot path
    // needs is created by the binary shell and handed to `pg_main` as
    // `Mcx<'static>`. There is therefore nothing for `MemoryContextInit` to
    // bootstrap here, so the faithful body is a successful no-op. Homed on the
    // mmgr-family owner (portalmem) because the mcxt.c interrupt/logging surface
    // has no dedicated body crate in the mcx world and `mcx` itself cannot depend
    // on the seam crate (the seam crate depends on `mcx`).
    backend_utils_mmgr_mcxt_seams::memory_context_init::set(|| Ok(()));

    // `TopMemoryContext` / `MemoryContextSwitchTo(TopMemoryContext)` for the
    // postmaster-forked child path (see `crate::top_context`). The single-user
    // backend never needs these (the binary shell threads `Mcx<'static>` into
    // `pg_main` explicitly); a forked child enters through
    // `postmaster_child_launch` with no handle to thread and calls these seams
    // as the equivalent of C's child `MemoryContextInit` +
    // `MemoryContextSwitchTo(TopMemoryContext)` at `*Main` entry. The body
    // establishes a per-process root `TopMemoryContext` on first call. Homed on
    // the mmgr-family owner (portalmem) alongside `memory_context_init`, because
    // mcxt.c has no dedicated body crate in the mcx world and `mcx` itself
    // cannot depend on the seam crate.
    backend_utils_mmgr_mcxt_seams::top_memory_context::set(
        crate::top_context::top_memory_context,
    );
    backend_utils_mmgr_mcxt_seams::switch_to_top_memory_context::set(
        crate::top_context::switch_to_top_memory_context,
    );

    // The mcxt.c memory-context-logging interrupt trio
    // (`HandleLogMemoryContextInterrupt` / `ProcessLogMemoryContextInterrupt` +
    // the `LogMemoryContextPending` read). Homed here alongside the
    // TopMemoryContext substrate (`crate::top_context`) because the dump targets
    // `MemoryContextStatsDetail(TopMemoryContext, ...)` and mcxt.c has no
    // dedicated body crate in the mcx world. The READ is default-false (the
    // happy path never sets the flag, so this unblocks every aux-process *Main
    // loop and CHECK_FOR_INTERRUPTS). HANDLE raises InterruptPending +
    // sets the per-backend pending flag; PROCESS clears the flag and emits the
    // per-context LOG_SERVER_ONLY stats dump.
    backend_utils_mmgr_mcxt_seams::log_memory_context_pending::set(
        crate::top_context::log_memory_context_pending,
    );
    backend_utils_mmgr_mcxt_seams::handle_log_memory_context_interrupt::set(
        crate::top_context::handle_log_memory_context_interrupt,
    );
    backend_utils_mmgr_mcxt_seams::process_log_memory_context_interrupt::set(
        crate::top_context::process_log_memory_context_interrupt,
    );
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
