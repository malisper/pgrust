//! Port of the small `src/backend/utils/activity/` files (PostgreSQL 18.3):
//!
//! * `backend_progress.c` — command progress reporting ([`backend_progress`])
//! * `pgstat_archiver.c` — archiver statistics ([`pgstat_archiver`])
//! * `pgstat_bgwriter.c` — bgwriter statistics ([`pgstat_bgwriter`])
//! * `pgstat_checkpointer.c` — checkpointer statistics ([`pgstat_checkpointer`])

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod backend_progress;
pub mod fmgr_builtins;
pub mod pgstat_archiver;
pub mod pgstat_bgwriter;
pub mod pgstat_checkpointer;

mod changecount;

pub use backend_progress::*;
pub use pgstat_archiver::*;
pub use pgstat_bgwriter::*;
pub use pgstat_checkpointer::*;

/// Install this crate's `backend_progress.c` inward seams.
pub fn init_seams() {
    use ::types_pgstat::backend_progress::ProgressCommandType;

    // Register the global checkpointer/bgwriter pg_stat_get_* SQL accessors
    // (pgstatfuncs.c).
    fmgr_builtins::register_pgstat_checkpointer_bgwriter_builtins();

    // `pgstat_progress_start_command(cmdtype, relid)`. The seam carries
    // `cmdtype` as the raw `int` the C `ProgressCommandType` enum value
    // (`utils/backend_progress.h`); map it back to the enum. The C entry point
    // is infallible (`void`), so wrap in `Ok`.
    activity_small_seams::pgstat_progress_start_command::set(|cmdtype, relid| {
        let cmdtype = match cmdtype {
            0 => ProgressCommandType::Invalid,
            1 => ProgressCommandType::Vacuum,
            2 => ProgressCommandType::Analyze,
            3 => ProgressCommandType::Cluster,
            4 => ProgressCommandType::CreateIndex,
            5 => ProgressCommandType::Basebackup,
            6 => ProgressCommandType::Copy,
            other => panic!("pgstat_progress_start_command: invalid ProgressCommandType {other}"),
        };
        backend_progress::pgstat_progress_start_command(cmdtype, relid);
        Ok(())
    });
    activity_small_seams::pgstat_progress_update_param::set(|index, val| {
        backend_progress::pgstat_progress_update_param(index, val);
        Ok(())
    });
    activity_small_seams::pgstat_progress_end_command::set(|| {
        backend_progress::pgstat_progress_end_command();
        Ok(())
    });

    // The properly-typed `backend_progress.c` seam family (carries the
    // `ProgressCommandType` enum directly, is infallible like the C `void`
    // entry points, and exposes `pgstat_progress_update_multi_param` which the
    // i32-flavored `small-seams` family lacks). Consumers: backend-catalog-index
    // (CREATE INDEX / CLUSTER progress) and backend-commands-copyto (COPY TO
    // progress). These bodies are this crate's `backend_progress` port verbatim.
    backend_progress_seams::pgstat_progress_start_command::set(
        |cmdtype, relid| backend_progress::pgstat_progress_start_command(cmdtype, relid),
    );
    backend_progress_seams::pgstat_progress_update_param::set(|index, val| {
        backend_progress::pgstat_progress_update_param(index, val);
    });
    backend_progress_seams::pgstat_progress_update_multi_param::set(
        |index, val| backend_progress::pgstat_progress_update_multi_param(index, val),
    );
    backend_progress_seams::pgstat_progress_end_command::set(|| {
        backend_progress::pgstat_progress_end_command();
    });

    // Parallel-worker message handling forwards an incremental progress update
    // from a worker to the leader's backend status entry (parallel.c
    // HandleParallelMessage `pgstat_progress_incr_param(index, incr)`). The body
    // is backend_progress.c's `pgstat_progress_incr_param` (void); install the
    // parallel-rt slot from the real owner. The parallel-rt seam crate is a leaf
    // (no cycle).
    parallel_rt_seams::pgstat_progress_incr_param::set(|index, incr| {
        backend_progress::pgstat_progress_incr_param(index, incr);
        Ok(())
    });

    // --- lazy-vacuum driver progress reporting (vacuumlazy.c). These home in
    //     vacuumlazy-seams; backend_progress.c owns the bodies. ---
    {
        use vacuumlazy_seams as vx;
        vx::pgstat_progress_start_command::set(|cmdtype, relid| {
            let cmdtype = match cmdtype {
                0 => ProgressCommandType::Invalid,
                1 => ProgressCommandType::Vacuum,
                2 => ProgressCommandType::Analyze,
                3 => ProgressCommandType::Cluster,
                4 => ProgressCommandType::CreateIndex,
                5 => ProgressCommandType::Basebackup,
                6 => ProgressCommandType::Copy,
                other => {
                    panic!("pgstat_progress_start_command: invalid ProgressCommandType {other}")
                }
            };
            backend_progress::pgstat_progress_start_command(cmdtype, relid);
            Ok(())
        });
        vx::pgstat_progress_update_param::set(|index, val| {
            backend_progress::pgstat_progress_update_param(index, val);
            Ok(())
        });
        vx::pgstat_progress_update_multi_param::set(|index, val| {
            backend_progress::pgstat_progress_update_multi_param(&index, &val);
            Ok(())
        });
        vx::pgstat_progress_end_command::set(|| {
            backend_progress::pgstat_progress_end_command();
            Ok(())
        });
        vx::my_be_entry_progress_param::set(|idx| {
            Ok(backend_progress::pgstat_read_progress_param(idx))
        });
    }
}

#[cfg(test)]
mod test_seams;
