#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};

use init_small::globals::MaxBackends;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::ProcNumber;
use types_error::PgResult;
use types_storage::lock::{LOCKMODE, LOCKTAG};

#[derive(Clone, Copy, Default)]
pub struct Edge {
    pub waiter: ProcNumber,
    pub blocker: ProcNumber,
    pub lock: LOCKTAG,
    pub pred: i32,
    pub link: i32,
}

#[derive(Clone, Copy, Default)]
pub struct DeadLockInfo {
    pub locktag: LOCKTAG,
    pub lockmode: LOCKMODE,
    pub pid: i32,
}

#[derive(Clone, Copy, Default)]
pub struct WaitOrder {
    pub lock: LOCKTAG,
    pub procs_offset: i32,
    pub nProcs: i32,
}

// DeadLockCheck workspace, preallocated so the check never allocates (C runs
// it off a timeout with all lock partition LWLocks held). visitedProcs
// doubles as topoProcs per C.
pub struct Workspace {
    pub visitedProcs: PgVec<'static, ProcNumber>,
    pub deadlockDetails: PgVec<'static, DeadLockInfo>,
    pub beforeConstraints: PgVec<'static, i32>,
    pub afterConstraints: PgVec<'static, i32>,
    pub waitOrders: PgVec<'static, WaitOrder>,
    pub waitOrderProcs: PgVec<'static, ProcNumber>,
    pub curConstraints: PgVec<'static, Edge>,
    pub possibleConstraints: PgVec<'static, Edge>,
    pub maxCurConstraints: i32,
    pub maxPossibleConstraints: i32,
}

thread_local! {
    static WORKSPACE: RefCell<Option<Workspace>> = const { RefCell::new(None) };
}

fn backend_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: Cell<Option<&'static MemoryContext>> = const { Cell::new(None) };
    }
    CTX.with(|c| match c.get() {
        Some(m) => m.mcx(),
        None => {
            let m: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("DeadLockChecking")));
            c.set(Some(m));
            m.mcx()
        }
    })
}

pub fn InitDeadLockChecking() -> PgResult<()> {
    let max_backends = MaxBackends() as usize;
    let mcx = backend_mcx();
    let ws = Workspace {
        visitedProcs: PgVec::with_capacity_in(max_backends, mcx),
        deadlockDetails: PgVec::with_capacity_in(max_backends, mcx),
        beforeConstraints: PgVec::with_capacity_in(max_backends, mcx),
        afterConstraints: PgVec::with_capacity_in(max_backends, mcx),
        waitOrders: PgVec::with_capacity_in(max_backends / 2, mcx),
        waitOrderProcs: PgVec::with_capacity_in(max_backends, mcx),
        curConstraints: PgVec::with_capacity_in(max_backends, mcx),
        possibleConstraints: PgVec::with_capacity_in(max_backends * 4, mcx),
        maxCurConstraints: MaxBackends(),
        maxPossibleConstraints: MaxBackends() * 4,
    };
    WORKSPACE.with(|w| *w.borrow_mut() = Some(ws));
    Ok(())
}

pub fn init_seams() {
    deadlock_seams::init_dead_lock_checking::set(InitDeadLockChecking);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_preallocates_c_sizes() {
        init_seams();
        init_small::globals::SetMaxBackends(16);
        deadlock_seams::init_dead_lock_checking::call().unwrap();
        WORKSPACE.with(|w| {
            let w = w.borrow();
            let ws = w.as_ref().unwrap();
            assert!(ws.visitedProcs.capacity() >= 16);
            assert!(ws.waitOrders.capacity() >= 8);
            assert!(ws.possibleConstraints.capacity() >= 64);
            assert_eq!(ws.maxCurConstraints, 16);
        });
        // DeadLockCheck/DeadLockReport stay loud until the checker ports.
        assert!(!deadlock_seams::dead_lock_check::is_installed());
        assert!(!deadlock_seams::dead_lock_report::is_installed());
    }
}
