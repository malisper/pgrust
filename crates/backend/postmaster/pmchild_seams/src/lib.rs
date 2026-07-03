use types_core::init::BackendType;
use types_core::pid_t;

// Owner: pmchild.c (the postmaster child-slot slab + ActiveChildList). Under
// the thread model this unit also owns child-exit notification (no SIGCHLD);
// see launch_backend's module doc. The `u32` masks are BackendTypeMask
// (1 << BackendType).

/// PMChild identity handed back to postmaster.c: the pmsignal child slot.
pub type PmChildSlot = i32;

seam_core::seam!(
    // InitPostmasterChildSlots (pmchild.c).
    pub fn init_postmaster_child_slots()
);

seam_core::seam!(
    // MaxLivePostmasterChildren (pmchild.c); panics before
    // InitPostmasterChildSlots ran (C elog(ERROR)); sizes pmsignal's flags.
    pub fn max_live_postmaster_children() -> i32
);

seam_core::seam!(
    // AssignPostmasterChildSlot(btype); None is C's NULL (no free slot).
    pub fn assign_postmaster_child_slot(btype: BackendType) -> Option<PmChildSlot>
);

seam_core::seam!(
    // AllocDeadEndChild(); None is C's NULL (out of memory).
    pub fn alloc_dead_end_child() -> Option<PmChildSlot>
);

seam_core::seam!(
    // ReleasePostmasterChildSlot(bp).
    pub fn release_postmaster_child_slot(child_slot: PmChildSlot) -> bool
);

seam_core::seam!(
    // bn->pid = pid after a successful launch.
    pub fn set_child_pid(child_slot: PmChildSlot, pid: pid_t)
);

seam_core::seam!(
    // CountChildren(targetMask) (postmaster.c walk over pmchild's ActiveChildList).
    pub fn count_children(target_mask: u32) -> i32
);

seam_core::seam!(
    // SignalChildren(signal, targetMask); returns whether any child was
    // signaled. Signal delivery to backend THREADS is this owner's redesign.
    pub fn signal_children(signal: i32, target_mask: u32) -> bool
);

seam_core::seam!(
    // FindPostmasterChildByPid(pid) (pmchild.c); None = stray pid.
    pub fn find_postmaster_child_by_pid(pid: pid_t) -> Option<(PmChildSlot, BackendType)>
);
