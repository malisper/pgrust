//! Install every seam this crate owns (`backend-commands-cluster-seams`).
//! `seams-init` calls [`init_seams`] once at startup.

/// Install the CLUSTER / VACUUM FULL entry-point seams.
pub fn init_seams() {
    backend_commands_cluster_seams::cluster_rel::set(crate::cluster_rel);
    backend_commands_cluster_seams::check_index_is_clusterable::set(crate::check_index_is_clusterable);
    backend_commands_cluster_seams::mark_index_clustered::set(crate::mark_index_clustered);
    backend_commands_cluster_seams::make_new_heap::set(crate::make_new_heap);
    backend_commands_cluster_seams::finish_heap_swap::set(crate::finish_heap_swap);
}
