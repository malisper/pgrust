//! Install every seam this crate owns (`backend-commands-cluster-seams`).
//! `seams-init` calls [`init_seams`] once at startup.

/// Install the CLUSTER / VACUUM FULL entry-point seams.
pub fn init_seams() {
    backend_commands_cluster_seams::cluster_rel::set(crate::cluster_rel);
    backend_commands_cluster_seams::check_index_is_clusterable::set(crate::check_index_is_clusterable);
    backend_commands_cluster_seams::mark_index_clustered::set(crate::mark_index_clustered);
    backend_commands_cluster_seams::make_new_heap::set(crate::make_new_heap);
    backend_commands_cluster_seams::finish_heap_swap::set(crate::finish_heap_swap);

    // matview.c reaches make_new_heap / finish_heap_swap (cluster.c) through its
    // outward frontier seam crate; cluster owns the bodies. The constant
    // arguments C passes at the matview call sites (ExclusiveLock; the
    // is_system_catalog/swap_toast_by_content/check_constraints/is_internal
    // bools; RecentXmin and ReadNextMultiXactId() as the freeze cutoffs) are
    // marshaled here.
    {
        use backend_commands_matview_deps_seams as m;
        use types_storage::lock::ExclusiveLock;
        m::make_new_heap::set(|matview_oid, table_space, relam, relpersistence| {
            let ctx = mcx::MemoryContext::new("make_new_heap");
            crate::make_new_heap(
                ctx.mcx(),
                matview_oid,
                table_space,
                relam,
                relpersistence as u8,
                ExclusiveLock,
            )
        });
        m::finish_heap_swap::set(|matview_oid, oid_new_heap, relpersistence| {
            let ctx = mcx::MemoryContext::new("finish_heap_swap");
            let frozen_xid = backend_utils_time_snapmgr::RecentXmin();
            let cutoff_multi =
                backend_access_transam_multixact_seams::read_next_multixact_id::call()?;
            crate::finish_heap_swap(
                ctx.mcx(),
                matview_oid,
                oid_new_heap,
                false,
                false,
                true,
                true,
                frozen_xid,
                cutoff_multi,
                relpersistence as u8,
            )
        });
    }
}
