//! Install the high-fan-in inward seams the workspace consumes from the
//! cumulative-statistics core (`pgstat.c` / `pgstat_shmem.c`).
//!
//! Called from [`crate::init_seams`] after the per-kind callback registry has
//! been populated. Seams whose full data path needs an unported per-kind crate
//! (e.g. the per-relation count seams owned by `pgstat_relation.c`) are NOT
//! installed here — they belong to that owner crate and stay seam-and-panic
//! until it lands.

use pgstat_seams as pgstat_seam;
use activity_shmem_seams as shmem_seam;

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

    // pgstat_fetch_entry(kind, dboid, objid): the variable-numbered snapshot
    // fetch. Installed now that pgstat.c's variable-snapshot path is ported, so
    // the per-kind pgstat_fetch_stat_{dbentry,funcentry,tabentry,replslot,
    // subscription,backend} paths go live.
    pgstat_seam::pgstat_fetch_entry::set(pgcore::pgstat_fetch_entry);

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

    // pgstat_restore_stats() / pgstat_discard_stats(): WAL-startup stats-file
    // restore-on-clean-shutdown / discard-on-crash (StartupXLOG).
    pgstat_seam::pgstat_restore_stats::set(pgcore::pgstat_restore_stats);
    pgstat_seam::pgstat_discard_stats::set(pgcore::pgstat_discard_stats);

    // ---- pgstat_relation.c (relation-open stats gate) ----
    //
    // pgstat_init_relation(relid, relkind): the relation-open gate deciding
    // whether to count this relation's stats. The seam returns the bit the
    // caller stores into rel->pgstat_enabled.
    pgstat_seam::pgstat_init_relation::set(crate::pgstat_relation::pgstat_init_relation);

    // ---- fixed-kind shared/snapshot field accessors ----
    //
    // The per-kind fixed crates (`pgstat_archiver.c` / `pgstat_bgwriter.c` /
    // `pgstat_checkpointer.c`) reach their fixed-kind region through
    // `&pgStatLocal.shmem-><kind>` (shared, mutated under its changecount lock)
    // and `&pgStatLocal.snapshot.<kind>` (the materialized snapshot copy). The
    // core dispatches those reports/fetches without passing the control block,
    // so the per-kind crates reach them via these projection seams. Install the
    // production accessors over the live `pgStatLocal` here (this crate owns that
    // backend-local control), mirroring C's field projections.
    //
    // `pgStatLocal.shmem` is a `PgStat_ShmemControl *` in C: dereferencing it
    // before `StatsShmemInit`/attach is undefined (NULL deref). On the boot
    // path `stats_shmem_init` runs before any fixed kind reports, so `shmem` is
    // `Some`; if it is `None` the closures panic, matching C's undefined NULL
    // deref (never reached in a correct sequence).
    pgstat_seam::with_shmem_archiver::set(|f| {
        crate::local::with_local(|l| {
            f(&mut l
                .shmem
                .as_mut()
                .expect("pgStatLocal.shmem accessed before StatsShmemInit")
                .archiver)
        })
    });
    pgstat_seam::with_shmem_bgwriter::set(|f| {
        crate::local::with_local(|l| {
            f(&mut l
                .shmem
                .as_mut()
                .expect("pgStatLocal.shmem accessed before StatsShmemInit")
                .bgwriter)
        })
    });
    pgstat_seam::with_shmem_checkpointer::set(|f| {
        crate::local::with_local(|l| {
            f(&mut l
                .shmem
                .as_mut()
                .expect("pgStatLocal.shmem accessed before StatsShmemInit")
                .checkpointer)
        })
    });

    // `pgStatLocal.snapshot.<kind>` is a value-typed field of the always-present
    // `PgStat_Snapshot snapshot` (not a pointer), so it is reachable whenever
    // `pgStatLocal` exists, matching C's `&pgStatLocal.snapshot.<kind>`.
    pgstat_seam::with_snapshot_archiver::set(|f| {
        crate::local::with_local(|l| f(&mut l.snapshot.archiver))
    });
    pgstat_seam::with_snapshot_bgwriter::set(|f| {
        crate::local::with_local(|l| f(&mut l.snapshot.bgwriter))
    });
    pgstat_seam::with_snapshot_checkpointer::set(|f| {
        crate::local::with_local(|l| f(&mut l.snapshot.checkpointer))
    });

    // `pgStatLocal.shmem->is_shutdown` — read by the report-path `Assert`s. C
    // dereferences `pgStatLocal.shmem` (NULL before attach); faithfully panic on
    // `None` rather than substituting a value the C code never observes.
    pgstat_seam::shmem_is_shutdown::set(|| {
        crate::local::with_local(|l| {
            l.shmem
                .as_ref()
                .expect("pgStatLocal.shmem accessed before StatsShmemInit")
                .is_shutdown
        })
    });

    // `pgstat_assert_is_up()` (pgstat.c:1539 / pgstat_internal.h:590). Outside
    // `USE_ASSERT_CHECKING` this is the macro `#define pgstat_assert_is_up()
    // ((void)true)` — a pure no-op. This port builds without assertion checking,
    // so the faithful installation is the empty body; the report paths
    // (pgstat_report_stat etc.) call it as a cheap invariant guard that compiles
    // away in production. (In an assert build it would
    // `Assert(pgstat_is_initialized && !pgstat_is_shutdown)`.)
    pgstat_seam::assert_is_up::set(|| {});

    // ---- pgstat.c GUC variable backing (conf->variable accessors) ----
    //
    // `bool pgstat_track_counts` and `int pgstat_fetch_consistency` are plain
    // process-global GUC variables declared in pgstat.c, read directly from the
    // GUC slot (not the control file). Install the get/set accessors over this
    // crate's `thread_local` backing storage so the GUC engine's `.read()` /
    // `.write()` resolve to it.
    {
        use guc_tables::{vars, GucVarAccessors};
        vars::pgstat_track_counts.install(GucVarAccessors {
            get: crate::guc::track_counts,
            set: crate::guc::set_track_counts,
        });
        vars::pgstat_fetch_consistency.install(GucVarAccessors {
            get: crate::guc::fetch_consistency,
            set: crate::guc::set_fetch_consistency,
        });
        vars::pgstat_track_functions.install(GucVarAccessors {
            get: crate::guc::track_functions,
            set: crate::guc::set_track_functions,
        });

        // `assign_stats_fetch_consistency` (pgstat.c) — the assign hook for the
        // `stats_fetch_consistency` GUC. C wires this function pointer into the
        // config table at compile time; this unit owns it and installs the slot.
        ::guc_tables::hooks::assign_stats_fetch_consistency
            .install(crate::guc::assign_stats_fetch_consistency);
    }
}
