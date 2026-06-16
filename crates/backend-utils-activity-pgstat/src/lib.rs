//! Cumulative-statistics subsystem owner crate — the **F0 carrier**: the
//! backend-local [`PgStat_EntryRef`] model, the process-local control state
//! (`pgStatLocal`), the pending-flush list / entry-ref hash, and the per-kind
//! **callback registry** (`pgstat_kind_builtin_infos[]`) that `pgstat.c`
//! dispatches through.
//!
//! This crate does *not* yet port `pgstat.c` / `pgstat_shmem.c` core, nor the
//! 12 per-kind variable files — that is the follow-on. It provides the carrier
//! and registry *shape* the core and the per-kind crates build on:
//!
//! * [`entry_ref`] — `PgStat_EntryRef`, `PgStat_LocalState`,
//!   `PgStat_EntryRefHashEntry`, the `pgStatPending` list, the
//!   `pgStatEntryRefHash` table.
//! * [`kind_info`] — [`kind_info::PgStat_KindCallbacks`] (the function-pointer
//!   half of C's `PgStat_KindInfo`), [`kind_info::PgStat_KindInfoFull`], the
//!   builtin table, and the [`kind_info::KindInfoBuilder`] registration API.
//! * [`registry`] — the process-global table singleton plus
//!   [`registry::register_builtin_kind`] / [`registry::pgstat_get_kind_info`].
//!
//! ## Per-kind registration API
//!
//! Each per-kind owner crate, from its `init_seams()`, builds a
//! [`kind_info::KindInfoBuilder`] (scalar metadata + the `*_cb` closures it
//! provides) and calls [`registry::register`]. Once `pgstat.c` is ported it
//! calls [`registry::seal_kind_table`] after all kinds register, and dispatches
//! through [`registry::pgstat_get_kind_info`].
//!
//! As proof of shape, [`init_seams`] wires the three already-ported fixed kinds
//! (bgwriter / archiver / checkpointer, from `backend-utils-activity-small`)
//! into the table via the field-projection adapter pattern.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod entry_ref;
pub mod kind_info;
pub mod registry;

use types_pgstat::activity_pgstat::{
    PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BGWRITER, PGSTAT_KIND_CHECKPOINTER,
};
use types_pgstat::pgstat_internal::PgStat_KindInfo;

use kind_info::KindInfoBuilder;

/// Build the scalar metadata for a fixed-numbered builtin kind.
///
/// `shared_size` / `shared_data_len` are taken from the real shared-entry types
/// via `size_of`. The `*_off` fields are C `offsetof` values used by the C
/// `void *`-based serialization and snapshot-pointer arithmetic; the idiomatic
/// port replaces that pointer math with typed field-projection adapters (see
/// [`kind_info`]), so the offsets carry no meaning here and are left 0. They are
/// retained as struct fields for field-for-field fidelity with C and will be
/// populated only if/when an on-disk-compatible serializer needs them.
fn fixed_kind_info(
    name: &'static str,
    shared_size: usize,
    shared_data_len: usize,
    accessed_across_databases: bool,
    write_to_file: bool,
) -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: true,
        accessed_across_databases,
        write_to_file,
        shared_size: shared_size as u32,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: 0,
        shared_data_len: shared_data_len as u32,
        pending_size: 0,
        name,
    }
}

/// Register this crate's per-kind callbacks, proving the table shape with the
/// three already-ported fixed kinds.
///
/// Real per-kind owner crates will each contribute their own
/// [`registry::register`] call from their own `init_seams()`; here we register
/// bgwriter / archiver / checkpointer (owned by `backend-utils-activity-small`)
/// as the proof of shape. The `init_shmem_cb` / `reset_all_cb` / `snapshot_cb`
/// adapters project the kind's field out of the owner
/// [`PgStat_ShmemControl`](types_pgstat::pgstat_internal::PgStat_ShmemControl) /
/// [`PgStat_Snapshot`](types_pgstat::pgstat_internal::PgStat_Snapshot) and call
/// the typed `*_cb` in `backend-utils-activity-small`.
pub fn init_seams() {
    use backend_utils_activity_small::pgstat_archiver as archiver;
    use backend_utils_activity_small::pgstat_bgwriter as bgwriter;
    use backend_utils_activity_small::pgstat_checkpointer as checkpointer;
    use types_pgstat::backend_utils_activity_pgstat_bgwriter::{
        PgStatShared_BgWriter, PgStat_BgWriterStats,
    };
    use types_pgstat::pgstat_internal::{PgStatShared_Archiver, PgStatShared_Checkpointer};

    // [PGSTAT_KIND_BGWRITER]
    registry::register(
        KindInfoBuilder::new(
            PGSTAT_KIND_BGWRITER,
            fixed_kind_info(
                "bgwriter",
                core::mem::size_of::<PgStatShared_BgWriter>(),
                core::mem::size_of::<PgStat_BgWriterStats>(),
                false,
                true,
            ),
        )
        .init_shmem_cb(|ctl| bgwriter::pgstat_bgwriter_init_shmem_cb(&mut ctl.bgwriter))
        .reset_all_cb(|_ctl, ts| bgwriter::pgstat_bgwriter_reset_all_cb(ts))
        .snapshot_cb(|_shmem, _snap| bgwriter::pgstat_bgwriter_snapshot_cb()),
    );

    // [PGSTAT_KIND_ARCHIVER]
    registry::register(
        KindInfoBuilder::new(
            PGSTAT_KIND_ARCHIVER,
            fixed_kind_info(
                "archiver",
                core::mem::size_of::<PgStatShared_Archiver>(),
                core::mem::size_of::<
                    types_pgstat::activity_pgstat::PgStat_ArchiverStats,
                >(),
                false,
                true,
            ),
        )
        .init_shmem_cb(|ctl| archiver::pgstat_archiver_init_shmem_cb(&mut ctl.archiver))
        .reset_all_cb(|_ctl, ts| archiver::pgstat_archiver_reset_all_cb(ts))
        .snapshot_cb(|_shmem, _snap| archiver::pgstat_archiver_snapshot_cb()),
    );

    // [PGSTAT_KIND_CHECKPOINTER]
    registry::register(
        KindInfoBuilder::new(
            PGSTAT_KIND_CHECKPOINTER,
            fixed_kind_info(
                "checkpointer",
                core::mem::size_of::<PgStatShared_Checkpointer>(),
                core::mem::size_of::<
                    types_pgstat::activity_pgstat::PgStat_CheckpointerStats,
                >(),
                false,
                true,
            ),
        )
        .init_shmem_cb(|ctl| {
            checkpointer::pgstat_checkpointer_init_shmem_cb(&mut ctl.checkpointer)
        })
        .reset_all_cb(|_ctl, ts| checkpointer::pgstat_checkpointer_reset_all_cb(ts))
        .snapshot_cb(|_shmem, _snap| checkpointer::pgstat_checkpointer_snapshot_cb()),
    );
}

#[cfg(test)]
mod tests;
