//! Port of the `backend-utils-init-small` unit:
//! `src/backend/utils/init/globals.c` (the backend-global variable
//! declarations) and `src/backend/utils/init/usercontext.c` (run code as a
//! different database user).

#![allow(non_snake_case)]

pub mod globals;
pub mod usercontext;

pub use usercontext::{RestoreUserContext, SwitchToUntrustedUser};

/// Install this unit's seams (`backend-utils-init-small-seams`).
pub fn init_seams() {
    backend_utils_init_small_seams::work_mem::set(globals::work_mem);
    backend_utils_init_small_seams::max_worker_processes::set(globals::max_worker_processes);
    backend_utils_init_small_seams::max_parallel_workers::set(globals::max_parallel_workers);
    backend_utils_init_small_seams::my_proc_pid::set(globals::MyProcPid);
    backend_utils_init_small_seams::my_proc_number::set(globals::MyProcNumber);
    backend_utils_init_small_seams::is_under_postmaster::set(globals::IsUnderPostmaster);
    backend_utils_init_small_seams::max_backends::set(globals::MaxBackends);
    backend_utils_init_small_seams::with_my_proc_port::set(globals::with_my_proc_port_seam);
    backend_utils_init_small_seams::set_my_proc_port::set(|port| {
        globals::SetMyProcPort(Some(Box::new(port)))
    });
    backend_utils_init_small_seams::set_client_connection_lost::set(globals::SetClientConnectionLost);
    backend_utils_init_small_seams::set_interrupt_pending::set(globals::SetInterruptPending);
    backend_utils_init_small_seams::set_proc_die_pending::set(globals::SetProcDiePending);
    backend_utils_init_small_seams::set_query_cancel_pending::set(globals::SetQueryCancelPending);
    backend_utils_init_small_seams::set_interrupt_holdoff_count::set(globals::SetInterruptHoldoffCount);
    backend_utils_init_small_seams::hold_interrupts::set(globals::HoldInterrupts);
    backend_utils_init_small_seams::resume_interrupts::set(globals::ResumeInterrupts);
    backend_utils_init_small_seams::set_my_backend_type::set(globals::SetMyBackendType);
    backend_utils_init_small_seams::nbuffers::set(globals::NBuffers);
    backend_utils_init_small_seams::my_database_id::set(globals::MyDatabaseId);
    backend_utils_init_small_seams::my_database_table_space::set(globals::MyDatabaseTableSpace);
    backend_utils_init_small_seams::is_postmaster_environment::set(globals::IsPostmasterEnvironment);
    backend_utils_init_small_seams::set_my_pm_child_slot::set(globals::SetMyPMChildSlot);
    backend_utils_init_small_seams::set_my_client_socket::set(|cs| globals::SetMyClientSocket(Some(cs)));
    backend_utils_init_small_seams::start_critical_section::set(globals::StartCriticalSection);
    backend_utils_init_small_seams::end_critical_section::set(globals::EndCriticalSection);
    backend_utils_init_small_seams::exit_on_any_error::set(globals::ExitOnAnyError);
    backend_utils_init_small_seams::set_exit_on_any_error::set(globals::SetExitOnAnyError);
    backend_utils_init_small_seams::my_backend_type::set(globals::MyBackendType);
}

#[cfg(test)]
mod tests;
