//! `storage/ipc/ipc.c` (unit: backend-storage-ipc) — exit-path state.

use core::cell::Cell;

thread_local! {
    /// `proc_exit_inprogress` (ipc.c).
    static PROC_EXIT_INPROGRESS: Cell<bool> = const { Cell::new(false) };
}

pub fn proc_exit_inprogress() -> bool {
    PROC_EXIT_INPROGRESS.with(Cell::get)
}
pub fn set_proc_exit_inprogress(value: bool) {
    PROC_EXIT_INPROGRESS.with(|c| c.set(value));
}
