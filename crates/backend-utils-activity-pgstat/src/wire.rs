//! Install the high-fan-in inward seams the workspace consumes from the
//! cumulative-statistics core (`pgstat.c` / `pgstat_shmem.c`).
//!
//! Called from [`crate::init_seams`] after the per-kind callback registry has
//! been populated. Seams whose full data path needs an unported per-kind crate
//! (e.g. the per-relation count seams owned by `pgstat_relation.c`) are NOT
//! installed here — they belong to that owner crate and stay seam-and-panic
//! until it lands.

use backend_utils_activity_pgstat_seams as pgstat_seam;
use backend_utils_activity_shmem_seams as shmem_seam;

use crate::pgstat_core as pgcore;
use crate::shmem;

/// Install the pgstat-core inward seams.
pub fn install_seams() {
    // ---- pgstat_shmem.c (backend-utils-activity-shmem) ----

    // stats_shmem_size / stats_shmem_init (ipci.c shmem bring-up).
    pgstat_seam::stats_shmem_size::set(shmem::stats_shmem_size);
    pgstat_seam::stats_shmem_init::set(shmem::stats_shmem_init);

    // pgstat_drop_entry(kind, dboid, objid).
    shmem_seam::pgstat_drop_entry::set(shmem::pgstat_drop_entry);

    // pgstat_request_entry_refs_gc().
    shmem_seam::pgstat_request_entry_refs_gc::set(|| {
        // Infallible seam; the runtime drops the Err (it cannot fire).
        let _ = shmem::pgstat_request_entry_refs_gc();
    });

    // pgstat_get_entry_ref_exists(kind, dboid, objid).
    shmem_seam::pgstat_get_entry_ref_exists::set(shmem::pgstat_get_entry_ref_exists);

    // ---- pgstat.c core (backend-utils-activity-pgstat) ----

    // pgstat_report_stat(force).
    pgstat_seam::pgstat_report_stat::set(pgcore::pgstat_report_stat);

    // pgstat_initialize().
    pgstat_seam::pgstat_initialize::set(pgcore::pgstat_initialize);

    // pgstat_before_server_shutdown(code, arg).
    pgstat_seam::pgstat_before_server_shutdown::set(pgcore::pgstat_before_server_shutdown);

    // pgstat_snapshot_fixed(kind) / snapshot_fixed.
    pgstat_seam::snapshot_fixed::set(pgcore::pgstat_snapshot_fixed);

    // pgstat_clear_snapshot().
    pgstat_seam::pgstat_clear_snapshot::set(pgcore::pgstat_clear_snapshot);

    // pgstat_reset(kind, dboid, objid).
    pgstat_seam::pgstat_reset::set(pgcore::pgstat_reset);

    // pgstat_reset_entry(kind, dboid, objid, ts).
    pgstat_seam::pgstat_reset_entry::set(pgcore::pgstat_reset_entry);

    // pgstat_get_kind_name(kind): the human-readable name from the kind table.
    pgstat_seam::pgstat_get_kind_name::set(|kind| {
        crate::registry::pgstat_get_kind_info(kind)
            .map(|ki| ki.info.name)
            .unwrap_or("???")
    });
}
