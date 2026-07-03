#![allow(non_snake_case)]

use std::cell::Cell;

use types_core::SubTransactionId;
use types_error::PgResult;

thread_local! {
    // Set only by worker launch (ParallelWorkerMain), which is unported.
    static PARALLEL_WORKER_NUMBER: Cell<i32> = const { Cell::new(-1) };
    // pcxt_list existence bit; its only writer (CreateParallelContext) is unported.
    static PCXT_LIST_NONEMPTY: Cell<bool> = const { Cell::new(false) };
}

pub fn ParallelWorkerNumber() -> i32 {
    PARALLEL_WORKER_NUMBER.with(|c| c.get())
}

pub fn IsParallelWorker() -> bool {
    ParallelWorkerNumber() >= 0
}

pub fn AtEOXact_Parallel(_is_commit: bool) -> PgResult<()> {
    if PCXT_LIST_NONEMPTY.with(|c| c.get()) {
        panic!("AtEOXact_Parallel: nonempty pcxt_list but DestroyParallelContext (access/transam/parallel.c) is not ported");
    }
    Ok(())
}

pub fn AtEOSubXact_Parallel(_is_commit: bool, _my_subid: SubTransactionId) -> PgResult<()> {
    if PCXT_LIST_NONEMPTY.with(|c| c.get()) {
        panic!("AtEOSubXact_Parallel: nonempty pcxt_list but DestroyParallelContext (access/transam/parallel.c) is not ported");
    }
    Ok(())
}

pub fn init_seams() {
    parallel_seams::is_parallel_worker::set(IsParallelWorker);
    parallel_seams::at_eoxact_parallel::set(AtEOXact_Parallel);
    parallel_seams::at_eosubxact_parallel::set(AtEOSubXact_Parallel);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_a_parallel_worker_and_empty_list_arms() {
        init_seams();
        assert!(!parallel_seams::is_parallel_worker::call());
        parallel_seams::at_eoxact_parallel::call(true).unwrap();
        parallel_seams::at_eoxact_parallel::call(false).unwrap();
        parallel_seams::at_eosubxact_parallel::call(true, 2).unwrap();
        assert!(!parallel_seams::parallel_worker_report_last_rec_end::is_installed());
    }
}
