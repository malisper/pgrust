#![allow(non_snake_case)]

pub mod globals;
pub mod usercontext;
pub mod wretain;

pub use usercontext::{RestoreUserContext, SwitchToUntrustedUser};

pub fn init_seams() {
    init_small_seams::my_proc_pid::set(globals::MyProcPid);

    // globals.c is the `conf->variable` backing store for these GUC slots.
    use guc_tables::{vars, GucVarAccessors};
    macro_rules! install_var {
        ($($slot:ident: $get:ident / $set:ident;)+) => {
            $(vars::$slot.install(GucVarAccessors {
                get: globals::$get,
                set: globals::$set,
            });)+
        };
    }
    // ExitOnAnyError's GUC backing lives in elog::config (its only readers).
    install_var! {
        NBuffers: NBuffers / SetNBuffers;
        MaxConnections: MaxConnections / SetMaxConnections;
        max_worker_processes: max_worker_processes / set_max_worker_processes;
        max_parallel_workers: max_parallel_workers / set_max_parallel_workers;
        max_parallel_maintenance_workers:
            max_parallel_maintenance_workers / set_max_parallel_maintenance_workers;
        work_mem: work_mem / set_work_mem;
        maintenance_work_mem: maintenance_work_mem / set_maintenance_work_mem;
        hash_mem_multiplier: hash_mem_multiplier / set_hash_mem_multiplier;
        enableFsync: enableFsync / set_enableFsync;
        allowSystemTableMods: allowSystemTableMods / set_allowSystemTableMods;
        data_directory_mode: data_directory_mode / set_data_directory_mode;
        IntervalStyle: IntervalStyle / SetIntervalStyle;
        VacuumBufferUsageLimit: VacuumBufferUsageLimit / SetVacuumBufferUsageLimit;
        VacuumCostPageHit: VacuumCostPageHit / SetVacuumCostPageHit;
        VacuumCostPageMiss: VacuumCostPageMiss / SetVacuumCostPageMiss;
        VacuumCostPageDirty: VacuumCostPageDirty / SetVacuumCostPageDirty;
        VacuumCostLimit: VacuumCostLimit / SetVacuumCostLimit;
        VacuumCostDelay: VacuumCostDelay / SetVacuumCostDelay;
        commit_timestamp_buffers: commit_timestamp_buffers / set_commit_timestamp_buffers;
        multixact_member_buffers: multixact_member_buffers / set_multixact_member_buffers;
        multixact_offset_buffers: multixact_offset_buffers / set_multixact_offset_buffers;
        notify_buffers: notify_buffers / set_notify_buffers;
        serializable_buffers: serializable_buffers / set_serializable_buffers;
        autovacuum_freeze_max_age: autovacuum_freeze_max_age / set_autovacuum_freeze_max_age;
        subtransaction_buffers: subtransaction_buffers / set_subtransaction_buffers;
        transaction_buffers: transaction_buffers / set_transaction_buffers;
    }
}

#[cfg(test)]
mod tests;
