use core::cell::Cell;

use init_small::globals;

// pgBufferUsage's shared_blks_* stores (instrument.h); pgstat_count_io_op /
// per-relation counts pend the pgstat unit.
thread_local! {
    static SHARED_BLKS_HIT: Cell<u64> = const { Cell::new(0) };
    static SHARED_BLKS_READ: Cell<u64> = const { Cell::new(0) };
    static SHARED_BLKS_DIRTIED: Cell<u64> = const { Cell::new(0) };
    static SHARED_BLKS_EVICTED: Cell<u64> = const { Cell::new(0) };
    static SHARED_BLKS_WRITTEN: Cell<u64> = const { Cell::new(0) };
    static LOCAL_BLKS_HIT: Cell<u64> = const { Cell::new(0) };
    static LOCAL_BLKS_READ: Cell<u64> = const { Cell::new(0) };
    static LOCAL_BLKS_DIRTIED: Cell<u64> = const { Cell::new(0) };
    static LOCAL_BLKS_WRITTEN: Cell<u64> = const { Cell::new(0) };
}

#[inline]
pub(crate) fn local_hit() {
    LOCAL_BLKS_HIT.with(|c| c.set(c.get() + 1));
    if globals::VacuumCostActive() {
        globals::SetVacuumCostBalance(globals::VacuumCostBalance() + globals::VacuumCostPageHit());
    }
}

#[inline]
pub(crate) fn local_read() {
    LOCAL_BLKS_READ.with(|c| c.set(c.get() + 1));
    if globals::VacuumCostActive() {
        globals::SetVacuumCostBalance(globals::VacuumCostBalance() + globals::VacuumCostPageMiss());
    }
}

#[inline]
pub(crate) fn local_dirtied() {
    LOCAL_BLKS_DIRTIED.with(|c| c.set(c.get() + 1));
}

#[inline]
pub(crate) fn local_written() {
    LOCAL_BLKS_WRITTEN.with(|c| c.set(c.get() + 1));
}

pub fn local_blks_hit() -> u64 {
    LOCAL_BLKS_HIT.with(|c| c.get())
}

pub fn local_blks_read() -> u64 {
    LOCAL_BLKS_READ.with(|c| c.get())
}

pub fn local_blks_dirtied() -> u64 {
    LOCAL_BLKS_DIRTIED.with(|c| c.get())
}

pub fn local_blks_written() -> u64 {
    LOCAL_BLKS_WRITTEN.with(|c| c.get())
}

#[inline]
pub(crate) fn hit() {
    SHARED_BLKS_HIT.with(|c| c.set(c.get() + 1));
    if globals::VacuumCostActive() {
        globals::SetVacuumCostBalance(globals::VacuumCostBalance() + globals::VacuumCostPageHit());
    }
}

#[inline]
pub(crate) fn read() {
    SHARED_BLKS_READ.with(|c| c.set(c.get() + 1));
    if globals::VacuumCostActive() {
        globals::SetVacuumCostBalance(globals::VacuumCostBalance() + globals::VacuumCostPageMiss());
    }
}

#[inline]
pub(crate) fn read_n(n: u64) {
    SHARED_BLKS_READ.with(|c| c.set(c.get() + n));
    if globals::VacuumCostActive() {
        globals::SetVacuumCostBalance(
            globals::VacuumCostBalance() + n as i32 * globals::VacuumCostPageMiss(),
        );
    }
}

#[inline]
pub(crate) fn dirtied() {
    SHARED_BLKS_DIRTIED.with(|c| c.set(c.get() + 1));
}

#[inline]
pub(crate) fn evict() {
    SHARED_BLKS_EVICTED.with(|c| c.set(c.get() + 1));
}

#[inline]
pub(crate) fn written() {
    SHARED_BLKS_WRITTEN.with(|c| c.set(c.get() + 1));
}

pub fn shared_blks_written() -> u64 {
    SHARED_BLKS_WRITTEN.with(|c| c.get())
}

pub fn shared_blks_hit() -> u64 {
    SHARED_BLKS_HIT.with(|c| c.get())
}

pub fn shared_blks_read() -> u64 {
    SHARED_BLKS_READ.with(|c| c.get())
}

pub fn shared_blks_dirtied() -> u64 {
    SHARED_BLKS_DIRTIED.with(|c| c.get())
}
