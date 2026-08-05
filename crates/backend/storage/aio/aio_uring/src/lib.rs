//! io_uring reads landing directly in shared-buffer pool pages
//! (method_io_uring.c + the StartReadBuffers read subset, collapsed to the
//! thread-per-backend model): bufmgr pins the victim and sets
//! BM_IO_IN_PROGRESS, we submit the SQE (wref armed first), and ANY thread may
//! drain a ring's completions (C's deadlock rule: whoever waits completes).
//! Divergence from C 18: availability-gated, not io_method-gated; fadvise
//! stays the fallback where the ring is absent.
//!
//! M1 §2.9 (parallelism-redesign): every in-flight slot carries a
//! `waiter::io::IoToken`; reaping completes it (unpark-all). Runtime-pool
//! worker rings are created at worker start (`uring_worker_ring_init`,
//! owner-submits-only), boundary-reaped at every task boundary
//! (`uring_boundary_reap`), and torn down at worker exit. WaitIO's wait arm
//! (`uring_buf_read_wait`) peeks first (permit-churn elision), then either
//! parks on the IoToken (foreign waiter, boundary-reaped ring) or does the
//! targeted blocking reap — both inside the §2.8 declared blocking section
//! (io_permit_release/reacquire → the runtime's IoGuard). Inert by default:
//! tokens only park when a runtime pool marked the ring, and the permit
//! seams are only installed by a running pool.

pub fn init_seams() {
    aio_seams::uring_buf_read::set(uring_buf_read);
    aio_seams::uring_buf_read_wait::set(uring_buf_read_wait);
    aio_seams::uring_collect_done::set(uring_collect_done);
    aio_seams::uring_drain_own::set(uring_drain_own);
    aio_seams::uring_available::set(uring_available);
    aio_seams::uring_drain_all_raw::set(uring_drain_all_raw);
    aio_seams::uring_worker_ring_init::set(uring_worker_ring_init);
    aio_seams::uring_worker_ring_teardown::set(uring_worker_ring_teardown);
    aio_seams::uring_boundary_reap::set(uring_boundary_reap);
}

#[cfg(not(target_os = "linux"))]
mod imp {
    pub fn uring_buf_read(_fd: i32, _offset: i64, _buffer: i32) -> bool {
        false
    }
    pub fn uring_buf_read_wait(_aio_index: u32, _generation: u64) {}
    pub fn uring_collect_done(_out: &mut [i32]) -> usize {
        0
    }
    pub fn uring_drain_own(_out: &mut [i32]) -> usize {
        0
    }
    pub fn uring_available() -> bool {
        false
    }
    pub fn uring_drain_all_raw() {}
    pub fn uring_worker_ring_init() -> i32 {
        -1
    }
    pub fn uring_worker_ring_teardown() {}
    pub fn uring_boundary_reap() {}
}

use imp::*;

#[cfg(target_os = "linux")]
#[doc(hidden)]
pub use imp::test_set_drop_token_complete_1in;

#[cfg(target_os = "linux")]
mod imp {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard};

    use elog::ereport;
    use types_error::{ErrorLocation, LOG};
    use waiter::io::IoToken;

    const ENTRIES: u32 = 128;
    const SLOTS: u32 = 128;
    const MAX_RINGS: usize = 1024;

    const IORING_OFF_SQ_RING: i64 = 0;
    const IORING_OFF_CQ_RING: i64 = 0x8000000;
    const IORING_OFF_SQES: i64 = 0x10000000;
    const IORING_ENTER_GETEVENTS: u32 = 1;
    const IORING_FEAT_SINGLE_MMAP: u32 = 1;
    const IORING_OP_READ: u8 = 22;

    #[repr(C)]
    struct SqOffsets {
        head: u32,
        tail: u32,
        ring_mask: u32,
        ring_entries: u32,
        flags: u32,
        dropped: u32,
        array: u32,
        resv1: u32,
        user_addr: u64,
    }

    #[repr(C)]
    struct CqOffsets {
        head: u32,
        tail: u32,
        ring_mask: u32,
        ring_entries: u32,
        overflow: u32,
        cqes: u32,
        flags: u32,
        resv1: u32,
        user_addr: u64,
    }

    #[repr(C)]
    struct UringParams {
        sq_entries: u32,
        cq_entries: u32,
        flags: u32,
        sq_thread_cpu: u32,
        sq_thread_idle: u32,
        features: u32,
        wq_fd: u32,
        resv: [u32; 3],
        sq_off: SqOffsets,
        cq_off: CqOffsets,
    }

    #[repr(C)]
    struct Sqe {
        opcode: u8,
        flags: u8,
        ioprio: u16,
        fd: i32,
        off: u64,
        addr: u64,
        len: u32,
        rw_flags: u32,
        user_data: u64,
        buf_index: u16,
        personality: u16,
        splice_fd_in: i32,
        _pad2: [u64; 2],
    }

    #[repr(C)]
    struct Cqe {
        user_data: u64,
        res: i32,
        flags: u32,
    }

    #[derive(Clone, Default)]
    struct Slot {
        buffer: i32,
        gen: u64,
        /// §2.9 IoToken for the in-flight read: created at submit, completed
        /// (unpark-all) by whichever thread reaps the CQE, dropped at
        /// collect/reuse. None once reaped or while the slot is free.
        token: Option<Arc<IoToken>>,
    }

    struct RingState {
        alive: bool,
        /// True for runtime-pool worker rings (uring_worker_ring_init): the
        /// owner drains CQEs at every task boundary, so foreign WaitIO
        /// waiters may park on the slot's IoToken instead of blocking-
        /// reaping this ring.
        boundary_reaper: bool,
        fd: i32,
        sq_ptr: *mut u8,
        sq_len: usize,
        cq_ptr: *mut u8,
        cq_len: usize,
        sqes: *mut Sqe,
        sqes_len: usize,
        sq_tail: *mut u32,
        sq_mask: u32,
        sq_array: *mut u32,
        cq_head: *mut u32,
        cq_tail: *const u32,
        cq_mask: u32,
        cqes: *const Cqe,
        free: u128,
        done: u128,
        inflight: u32,
        next_gen: u64,
        slots: [Slot; SLOTS as usize],
    }

    // SAFETY: ring pointers are touched only under the registry Mutex and only
    // while `alive`; head/tail words shared with the kernel go through atomics.
    unsafe impl Send for RingState {}

    static REGISTRY: [AtomicPtr<Mutex<RingState>>; MAX_RINGS] =
        [const { AtomicPtr::new(std::ptr::null_mut()) }; MAX_RINGS];
    static NEXT_RING: AtomicU32 = AtomicU32::new(0);

    thread_local! {
        // -1 unstarted, -2 unavailable, >=0 registry index of this thread's ring.
        static RING_ID: Cell<i32> = const { Cell::new(-1) };
    }

    fn errno() -> i32 {
        std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
    }

    #[cold]
    fn log_fallback(what: &str, e: i32) {
        static LOGGED: AtomicBool = AtomicBool::new(false);
        if !LOGGED.swap(true, Ordering::Relaxed) {
            let _ = ereport(LOG)
                .errmsg_internal(format!(
                    "io_uring buffer reads unavailable ({what}: errno {e}); falling back to posix_fadvise readahead"
                ))
                .finish(ErrorLocation::new(file!(), line!() as i32, "uring_buf_read"));
        }
    }

    fn enter(fd: i32, to_submit: u32, min_complete: u32, flags: u32) -> i64 {
        // SAFETY: plain io_uring_enter; no pointer arguments are passed.
        unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                fd,
                to_submit,
                min_complete,
                flags,
                std::ptr::null::<libc::c_void>(),
                0usize,
            ) as i64
        }
    }

    // SAFETY (module invariant): ring mmap pointers live until teardown, which
    // flips `alive` under the same mutex every deref holds.
    unsafe fn atomic_u32<'a>(p: *const u32) -> &'a AtomicU32 {
        unsafe { &*p.cast::<AtomicU32>() }
    }

    fn init_ring() -> Option<RingState> {
        // SAFETY: zeroed out-param for io_uring_setup.
        let mut p: UringParams = unsafe { std::mem::zeroed() };
        // SAFETY: syscall with a valid params pointer.
        let fd = unsafe { libc::syscall(libc::SYS_io_uring_setup, ENTRIES, &mut p) } as i32;
        if fd < 0 {
            log_fallback("io_uring_setup", errno());
            return None;
        }
        let close_fd = |fd: i32| {
            // SAFETY: fd is the ring fd we just created.
            unsafe { libc::close(fd) };
        };
        let sq_len_raw = p.sq_off.array as usize + p.sq_entries as usize * 4;
        let cq_len = p.cq_off.cqes as usize + p.cq_entries as usize * std::mem::size_of::<Cqe>();
        let single = p.features & IORING_FEAT_SINGLE_MMAP != 0;
        let sq_len = if single { sq_len_raw.max(cq_len) } else { sq_len_raw };
        let map = |len: usize, off: i64| -> *mut u8 {
            // SAFETY: mapping the ring fd regions the kernel defined in `p`.
            let m = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED | libc::MAP_POPULATE,
                    fd,
                    off,
                )
            };
            if m == libc::MAP_FAILED { std::ptr::null_mut() } else { m.cast() }
        };
        let sq_ptr = map(sq_len, IORING_OFF_SQ_RING);
        if sq_ptr.is_null() {
            log_fallback("mmap sq", errno());
            close_fd(fd);
            return None;
        }
        let cq_ptr = if single { sq_ptr } else { map(cq_len, IORING_OFF_CQ_RING) };
        let sqes_len = p.sq_entries as usize * std::mem::size_of::<Sqe>();
        let sqes = map(sqes_len, IORING_OFF_SQES);
        if cq_ptr.is_null() || sqes.is_null() {
            log_fallback("mmap", errno());
            close_fd(fd);
            return None;
        }
        // SAFETY: offsets come from the kernel's io_uring_params for these maps.
        unsafe {
            let sq_mask = *sq_ptr.add(p.sq_off.ring_mask as usize).cast::<u32>();
            let cq_mask = *cq_ptr.add(p.cq_off.ring_mask as usize).cast::<u32>();
            Some(RingState {
                alive: true,
                boundary_reaper: false,
                fd,
                sq_ptr,
                sq_len,
                cq_ptr,
                cq_len,
                sqes: sqes.cast(),
                sqes_len,
                sq_tail: sq_ptr.add(p.sq_off.tail as usize).cast(),
                sq_mask,
                sq_array: sq_ptr.add(p.sq_off.array as usize).cast(),
                cq_head: cq_ptr.add(p.cq_off.head as usize).cast(),
                cq_tail: cq_ptr.add(p.cq_off.tail as usize).cast(),
                cq_mask,
                cqes: cq_ptr.add(p.cq_off.cqes as usize).cast(),
                free: if SLOTS == 128 { u128::MAX } else { (1u128 << SLOTS) - 1 },
                done: 0,
                inflight: 0,
                next_gen: 1,
                slots: std::array::from_fn(|_| Slot::default()),
            })
        }
    }

    fn lock(h: &'static Mutex<RingState>) -> MutexGuard<'static, RingState> {
        h.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn own_ring() -> Option<(&'static Mutex<RingState>, u32)> {
        let id = RING_ID.get();
        if id >= 0 {
            // SAFETY: registry entries are leaked Boxes, never freed.
            return Some((unsafe { &*REGISTRY[id as usize].load(Ordering::Relaxed) }, id as u32));
        }
        if id == -2 {
            return None;
        }
        let Some(ring) = init_ring() else {
            RING_ID.set(-2);
            return None;
        };
        let idx = NEXT_RING.fetch_add(1, Ordering::Relaxed) as usize;
        if idx >= MAX_RINGS {
            log_fallback("ring registry full", 0);
            let mut r = ring;
            teardown(&mut r);
            RING_ID.set(-2);
            return None;
        }
        let handle: &'static Mutex<RingState> = Box::leak(Box::new(Mutex::new(ring)));
        REGISTRY[idx].store(handle as *const _ as *mut _, Ordering::Release);
        RING_ID.set(idx as i32);
        if ipc_seams::before_shmem_exit::is_installed()
            && ipc_seams::before_shmem_exit::call(shutdown_hook, datum::Datum::from_usize(idx))
                .is_err()
        {
            let mut st = lock(handle);
            teardown(&mut st);
            RING_ID.set(-2);
            return None;
        }
        Some((handle, idx as u32))
    }

    // Fault injection (§2.9 gate): PGRUST_TEST_URING_DROP_TOKEN_COMPLETE_1IN=N
    // drops every Nth IoToken completion at the reap site — exactly the
    // lost-completion shape (buffer state advanced, waiter wake dropped).
    // Parked waiters must recover through the wait protocol's recheck
    // backstop (state probe / degraded reap). u32::MAX = env not read yet.
    static DROP_TOKEN_COMPLETE_1IN: AtomicU32 = AtomicU32::new(u32::MAX);
    static DROP_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn drop_token_complete_fires() -> bool {
        let mut n = DROP_TOKEN_COMPLETE_1IN.load(Ordering::Relaxed);
        if n == u32::MAX {
            n = std::env::var("PGRUST_TEST_URING_DROP_TOKEN_COMPLETE_1IN")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&v| v != u32::MAX)
                .unwrap_or(0);
            DROP_TOKEN_COMPLETE_1IN.store(n, Ordering::Relaxed);
        }
        if n == 0 {
            return false;
        }
        DROP_TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed) % n as u64 == n as u64 - 1
    }

    /// Test override for the drop-completion injector (0 disables).
    #[doc(hidden)]
    pub fn test_set_drop_token_complete_1in(n: u32) {
        DROP_TOKEN_COUNTER.store(0, Ordering::Relaxed);
        DROP_TOKEN_COMPLETE_1IN.store(n, Ordering::Relaxed);
    }

    fn reap_locked(st: &mut RingState) {
        // SAFETY: module invariant (alive maps; head/tail via atomics).
        unsafe {
            let tail = atomic_u32(st.cq_tail).load(Ordering::Acquire);
            let mut head = atomic_u32(st.cq_head).load(Ordering::Relaxed);
            if head == tail {
                return;
            }
            while head != tail {
                let cqe = &*st.cqes.add((head & st.cq_mask) as usize);
                let slot = cqe.user_data as u32;
                debug_assert!(slot < SLOTS && st.free & (1u128 << slot) == 0);
                bufmgr::uring_read_complete(st.slots[slot as usize].buffer, cqe.res);
                st.done |= 1u128 << slot;
                st.inflight -= 1;
                head = head.wrapping_add(1);
                // §2.9: buffer state first (io_wref clear + TerminateBufferIO
                // + done bit), THEN IoToken complete → Waiter unpark-all, so
                // an unparked waiter always observes the settled state.
                if let Some(token) = st.slots[slot as usize].token.take() {
                    if !drop_token_complete_fires() {
                        token.complete();
                    }
                }
            }
            atomic_u32(st.cq_head).store(head, Ordering::Release);
        }
    }

    fn wait_locked(st: &mut RingState) {
        loop {
            let rc = enter(st.fd, 0, 1, IORING_ENTER_GETEVENTS);
            if rc >= 0 {
                return;
            }
            let e = errno();
            if e != libc::EINTR {
                // In-flight DMA targets pool pages; inventing a completion here
                // risks reuse-under-write. Loud beats corruption.
                panic!("io_uring_enter(GETEVENTS) failed: errno {e}");
            }
        }
    }

    fn collect_locked(st: &mut RingState, out: &mut [i32]) -> usize {
        let mut n = 0;
        while st.done != 0 && n < out.len() {
            let slot = st.done.trailing_zeros();
            let bit = 1u128 << slot;
            out[n] = st.slots[slot as usize].buffer;
            n += 1;
            st.done &= !bit;
            st.free |= bit;
        }
        n
    }

    pub fn uring_buf_read(fd: i32, offset: i64, buffer: i32) -> bool {
        let Some((handle, ring_id)) = own_ring() else {
            return false;
        };
        let mut st = lock(handle);
        if !st.alive {
            return false;
        }
        reap_locked(&mut st);
        if st.free == 0 {
            return false;
        }
        // O_DIRECT DMA contract: pool pages are PG_IO_ALIGN_SIZE-aligned and
        // whole blocks are 4k multiples, so addr/off/len all satisfy DIO.
        const _: () = assert!(types_core::BLCKSZ % 4096 == 0);
        debug_assert!(offset % 4096 == 0);
        debug_assert!(bufmgr::BufferGetBlockPtr(buffer) as usize % 4096 == 0);
        let slot = st.free.trailing_zeros();
        let gen = st.next_gen;
        st.next_gen += 1;
        // One IoToken per in-flight read (§2.9): cqe id = the slot
        // generation; completed by whichever thread reaps the CQE.
        st.slots[slot as usize] =
            Slot { buffer, gen, token: Some(Arc::new(IoToken::new(ring_id, gen))) };
        // Arm the wref before the SQE can complete: waiters route to this ring.
        bufmgr::uring_set_io_wref(buffer, ring_id * SLOTS + slot + 1, gen);
        // SAFETY: module invariant; idx masked into the SQE array; the slot bit
        // guarantees exclusive use of that page until its CQE.
        unsafe {
            let tail = atomic_u32(st.sq_tail).load(Ordering::Relaxed);
            let idx = tail & st.sq_mask;
            st.sqes.add(idx as usize).write(Sqe {
                opcode: IORING_OP_READ,
                flags: 0,
                ioprio: 0,
                fd,
                off: offset as u64,
                addr: bufmgr::BufferGetBlockPtr(buffer) as u64,
                len: types_core::BLCKSZ as u32,
                rw_flags: 0,
                user_data: slot as u64,
                buf_index: 0,
                personality: 0,
                splice_fd_in: 0,
                _pad2: [0; 2],
            });
            st.sq_array.add(idx as usize).write(idx);
            atomic_u32(st.sq_tail).store(tail.wrapping_add(1), Ordering::Release);
        }
        // Submit-immediately: the kernel takes its own file reference during
        // this enter, so a later vfd close cannot redirect the read.
        loop {
            let rc = enter(st.fd, 1, 0, 0);
            if rc >= 0 {
                break;
            }
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            bufmgr::uring_clear_io_wref(buffer);
            // No CQE will ever arrive for this slot: complete the token so a
            // racer that cloned it between the wref arming and this backout
            // can never park forever (registration was impossible while we
            // held the ring mutex, so this is belt-and-braces).
            if let Some(token) = st.slots[slot as usize].token.take() {
                token.complete();
            }
            log_fallback("io_uring_enter", e);
            teardown(&mut st);
            return false;
        }
        st.free &= !(1u128 << slot);
        st.inflight += 1;
        true
    }

    /// The IO settled from this waiter's point of view: ring dead, slot
    /// reused (stale generation), collected, or reaped-done. Mirrors the
    /// pre-M1 wait loop's exit predicate exactly.
    fn io_settled(st: &RingState, slot: usize, generation: u64) -> bool {
        let bit = 1u128 << slot;
        !st.alive
            || st.slots[slot].gen != generation
            || st.free & bit != 0
            || st.done & bit != 0
    }

    /// Targeted blocking reap on the owning ring (any-thread-completes):
    /// drain CQEs — running completions, including foreign slots' — until
    /// (slot, generation) settles.
    fn blocking_reap(handle: &'static Mutex<RingState>, slot: usize, generation: u64) {
        let mut st = lock(handle);
        loop {
            if io_settled(&st, slot, generation) {
                return;
            }
            reap_locked(&mut st);
            if st.done & (1u128 << slot) != 0 {
                return;
            }
            wait_locked(&mut st);
        }
    }

    pub fn uring_buf_read_wait(aio_index: u32, generation: u64) {
        if aio_index == 0 {
            return;
        }
        let idx = aio_index - 1;
        let (ring_id, slot) = ((idx / SLOTS) as usize, (idx % SLOTS) as usize);
        let p = REGISTRY[ring_id].load(Ordering::Acquire);
        if p.is_null() {
            return;
        }
        // SAFETY: registry entries are leaked, never freed.
        let handle: &'static Mutex<RingState> = unsafe { &*p };

        // Peek-complete (§2.9): one non-blocking reap of the owner ring. An
        // already-complete IO returns here — no token registration and NO
        // permit churn (the IoGuard elision).
        let token = {
            let mut st = lock(handle);
            if io_settled(&st, slot, generation) {
                return;
            }
            reap_locked(&mut st);
            if io_settled(&st, slot, generation) {
                return;
            }
            // Genuinely pending: pick the wait shape. Parking on the IoToken
            // is sound only when the ring's owner reaps at task boundaries
            // (runtime-pool rings) and we are not that owner; every other
            // case keeps the pre-M1 targeted blocking reap.
            if st.boundary_reaper && RING_ID.get() != ring_id as i32 {
                st.slots[slot].token.clone()
            } else {
                None
            }
        };

        // Declared blocking section (§2.8): a pool worker holding an
        // execution permit releases it here (a standby absorbs the core)
        // and reacquires after the wait — the runtime's IoGuard, reached
        // through the seam pair. Plain backends: seam uninstalled or
        // returns false, a no-op.
        let released = aio_seams::io_permit_release::is_installed()
            && aio_seams::io_permit_release::call();
        match token {
            Some(token) => {
                // Owner reaps at its task boundaries; park on the token.
                // Recheck cadence backstop: a lost completion is caught by
                // the settled-state probe, and a genuinely-unreaped IO
                // (owner parked idle / wedged) degrades to the targeted
                // blocking reap after one cadence.
                token.wait_with(
                    waiter::current_handle(),
                    waiter::park,
                    || io_settled(&lock(handle), slot, generation),
                    || blocking_reap(handle, slot, generation),
                );
            }
            None => blocking_reap(handle, slot, generation),
        }
        if released {
            aio_seams::io_permit_reacquire::call();
        }
    }

    pub fn uring_collect_done(out: &mut [i32]) -> usize {
        let id = RING_ID.get();
        if id < 0 {
            return 0;
        }
        // SAFETY: registry entries are leaked, never freed.
        let mut st = lock(unsafe { &*REGISTRY[id as usize].load(Ordering::Relaxed) });
        if st.alive {
            reap_locked(&mut st);
        }
        collect_locked(&mut st, out)
    }

    pub fn uring_drain_own(out: &mut [i32]) -> usize {
        let id = RING_ID.get();
        if id < 0 {
            return 0;
        }
        // SAFETY: registry entries are leaked, never freed.
        let mut st = lock(unsafe { &*REGISTRY[id as usize].load(Ordering::Relaxed) });
        while st.alive && st.inflight > 0 {
            reap_locked(&mut st);
            if st.inflight == 0 {
                break;
            }
            wait_locked(&mut st);
        }
        collect_locked(&mut st, out)
    }

    pub fn uring_available() -> bool {
        own_ring().is_some()
    }

    /// §2.9 ring topology for the runtime pool: eagerly create THIS worker
    /// thread's ring and mark it boundary-reaped (the owner drains CQEs at
    /// every task boundary, so WaitIO waiters may park on the IoToken).
    /// Returns the ring id for registration with the runtime worker struct,
    /// or -1 when uring is unavailable here.
    pub fn uring_worker_ring_init() -> i32 {
        let Some((handle, ring_id)) = own_ring() else {
            return -1;
        };
        let mut st = lock(handle);
        if !st.alive {
            return -1;
        }
        st.boundary_reaper = true;
        ring_id as i32
    }

    /// §2.9: pool-worker exit — wait out this thread's in-flight DMA
    /// (completions run, IoTokens complete), drop the issuer pins, then
    /// unmap and close the ring. Pin collection MUST precede teardown: the
    /// slot-held pins are this thread's and no one else will ever drop them.
    pub fn uring_worker_ring_teardown() {
        let id = RING_ID.get();
        if id < 0 {
            return;
        }
        bufmgr::uring_drain_pins();
        // SAFETY: registry entries are leaked, never freed.
        let mut st = lock(unsafe { &*REGISTRY[id as usize].load(Ordering::Relaxed) });
        teardown(&mut st);
    }

    /// §2.9 boundary duty: non-blocking drain of THIS thread's CQEs.
    /// Completions run (io_wref clear + TerminateBufferIO + IoToken
    /// complete → Waiter unpark-all) and collected issuer pins drop —
    /// bufmgr's collect/drain discipline, driven from the worker loop.
    pub fn uring_boundary_reap() {
        if RING_ID.get() < 0 {
            return;
        }
        bufmgr::uring_collect_pins();
    }

    pub fn uring_drain_all_raw() {
        let n = (NEXT_RING.load(Ordering::Relaxed) as usize).min(MAX_RINGS);
        for reg in REGISTRY.iter().take(n) {
            let p = reg.load(Ordering::Acquire);
            if p.is_null() {
                continue;
            }
            // SAFETY: registry entries are leaked, never freed.
            let mut st = lock(unsafe { &*p });
            if !st.alive {
                continue;
            }
            while st.inflight > 0 {
                // SAFETY: module invariant; raw reap — the pool is being reset,
                // so completions are dropped, only the DMA is waited out.
                unsafe {
                    let tail = atomic_u32(st.cq_tail).load(Ordering::Acquire);
                    let mut head = atomic_u32(st.cq_head).load(Ordering::Relaxed);
                    while head != tail {
                        st.inflight -= 1;
                        head = head.wrapping_add(1);
                    }
                    atomic_u32(st.cq_head).store(head, Ordering::Release);
                }
                if st.inflight > 0 {
                    wait_locked(&mut st);
                }
            }
            st.done = 0;
            st.free = if SLOTS == 128 { u128::MAX } else { (1u128 << SLOTS) - 1 };
            // Crash-cycle reset: drop the tokens WITHOUT completing them —
            // every child that could have parked on one is dead.
            for s in st.slots.iter_mut() {
                s.token = None;
            }
        }
    }

    fn teardown(st: &mut RingState) {
        let mut spins = 0;
        while st.alive && st.inflight > 0 {
            reap_locked(st);
            if st.inflight == 0 {
                break;
            }
            let rc = enter(st.fd, 0, st.inflight, IORING_ENTER_GETEVENTS);
            if rc < 0 && errno() != libc::EINTR {
                break;
            }
            spins += 1;
            if spins > 1000 {
                break;
            }
        }
        if !st.alive {
            return;
        }
        st.alive = false;
        // SAFETY: alive=false under the mutex; no deref of these maps can
        // happen after this point.
        unsafe {
            libc::munmap(st.sqes.cast(), st.sqes_len);
            if st.cq_ptr != st.sq_ptr {
                libc::munmap(st.cq_ptr.cast(), st.cq_len);
            }
            libc::munmap(st.sq_ptr.cast(), st.sq_len);
            libc::close(st.fd);
        }
    }

    fn shutdown_hook(_code: i32, arg: datum::Datum) -> types_error::PgResult<()> {
        let idx = arg.as_usize();
        let p = REGISTRY[idx].load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: registry entries are leaked, never freed.
            let mut st = lock(unsafe { &*p });
            teardown(&mut st);
        }
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::Once;

    use bufmgr::{
        AtEOXact_Buffers, GetPrivateRefCount, PrefetchOutcome, ReadBufferWithoutRelcache,
        ReleaseBuffer,
    };
    use init_small::globals;
    use types_core::{Buffer, ForkNumber, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
    use types_error::PgError;
    use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};

    const TEST_NBUFFERS: i32 = 64;
    const TEST_MAX_CONNECTIONS: i32 = 8;
    const URING_REL: u32 = 9600;
    const FILE_PAGES: u32 = 8;

    static SYNC_READS: AtomicI32 = AtomicI32::new(0);
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static URING_FILE: std::sync::OnceLock<std::fs::File> = std::sync::OnceLock::new();

    fn valid_page_into(buffer: &mut [u8], blkno: u32) {
        buffer.fill(0);
        let set_u16 =
            |b: &mut [u8], off: usize, v: u16| b[off..off + 2].copy_from_slice(&v.to_ne_bytes());
        set_u16(buffer, 12, 24);
        set_u16(buffer, 14, BLCKSZ as u16);
        set_u16(buffer, 16, BLCKSZ as u16);
        set_u16(buffer, 18, (BLCKSZ as u16) | 4);
        buffer[24..28].copy_from_slice(&blkno.to_ne_bytes());
    }

    static DIO_ENGAGED: AtomicBool = AtomicBool::new(false);

    // Plain-blkno pages for the pgaio sync path (rel.dat carries +100 so
    // uring-DMA'd pages stay distinguishable from sync arrivals). Grown on
    // demand: the old smgr_read fake stamped pages in memory (an infinite
    // disk), and the short-read test reads past FILE_PAGES — the sync
    // arrival must always find a full page here.
    static URING_SYNC_FILE: std::sync::Mutex<Option<std::fs::File>> =
        std::sync::Mutex::new(None);

    fn sync_file_fd(blocknum: u32, nblocks: u32) -> i32 {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::fd::AsRawFd;
        let mut guard = URING_SYNC_FILE.lock().unwrap();
        if guard.is_none() {
            let base = if std::path::Path::new("/work").is_dir() {
                std::path::PathBuf::from("/work")
            } else {
                std::env::temp_dir()
            };
            let dir = base.join(format!("aio-uring-pool-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("rel-sync.dat");
            *guard = Some(
                std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap(),
            );
        }
        let f = guard.as_mut().unwrap();
        let needed_end = (blocknum + nblocks) as u64 * BLCKSZ as u64;
        let cur = f.metadata().unwrap().len();
        if cur < needed_end {
            let first = (cur / BLCKSZ as u64) as u32;
            f.seek(SeekFrom::Start(first as u64 * BLCKSZ as u64)).unwrap();
            let mut page = vec![0u8; BLCKSZ];
            for b in first..(blocknum + nblocks) {
                valid_page_into(&mut page, b);
                f.write_all(&page).unwrap();
            }
            f.flush().unwrap();
        }
        f.as_raw_fd()
    }

    fn uring_file_fd() -> i32 {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;
        URING_FILE
            .get_or_init(|| {
                // /work is the fleet NVMe (O_DIRECT-capable); temp_dir may be tmpfs.
                let base = if std::path::Path::new("/work").is_dir() {
                    std::path::PathBuf::from("/work")
                } else {
                    std::env::temp_dir()
                };
                let dir = base.join(format!("aio-uring-pool-{}", std::process::id()));
                std::fs::create_dir_all(&dir).unwrap();
                let path = dir.join("rel.dat");
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let mut page = vec![0u8; BLCKSZ];
                for blk in 0..FILE_PAGES {
                    // +100 distinguishes uring-DMA'd pages from sync fallbacks.
                    valid_page_into(&mut page, blk + 100);
                    f.write_all(&page).unwrap();
                }
                f.flush().unwrap();
                f.sync_all().unwrap();
                match std::fs::OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&path)
                {
                    Ok(dio) => {
                        DIO_ENGAGED.store(true, Ordering::Relaxed);
                        dio
                    }
                    Err(e) => {
                        eprintln!("O_DIRECT open failed ({e}); pool-read tests run buffered");
                        f
                    }
                }
            })
            .as_raw_fd()
    }

    fn become_backend() {
        if globals::MyProcNumber() != INVALID_PROC_NUMBER {
            return;
        }
        static NEXT_PROCNO: AtomicI32 = AtomicI32::new(0);
        let procno = NEXT_PROCNO.fetch_add(1, Ordering::Relaxed);
        globals::SetMyProcNumber(procno);
        globals::SetMyProcPid(7000 + procno);
        waiteventset::InitializeWaitEventSupport().unwrap();
        let h = types_storage::latch::LatchHandle::proc(procno);
        latch::OwnLatch(h).unwrap();
        globals::SetMyLatch(Some(h));
        latch::InitializeLatchWaitSet().unwrap();
        let owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "uring-tests")
                .unwrap();
        resowner::SetCurrentResourceOwner(owner);
        // The read pipeline issues IO through pgaio: attach this thread's
        // aio backend slot (MyProc is the bind_task_proc TLS here).
        lmgr_proc::bind_task_proc(procno);
        aio_core::pgaio_init_backend();
    }

    // §2.8 permit-seam stand-ins (the runtime crate is not linked here, so
    // the slots are ours): count the release/reacquire pairing every wait
    // path must respect. Returning true = "this thread held a permit".
    static PERMIT_RELEASES: AtomicI32 = AtomicI32::new(0);
    static PERMIT_REACQUIRES: AtomicI32 = AtomicI32::new(0);

    fn setup() -> std::sync::MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            // Before ANY waiter park in this binary: a short recheck cadence
            // so the lost-completion backstop tests bound at ~50ms.
            std::env::set_var("PGRUST_WAITER_RECHECK_MS", "50");
            aio_seams::io_permit_release::set(|| {
                PERMIT_RELEASES.fetch_add(1, Ordering::SeqCst);
                true
            });
            aio_seams::io_permit_reacquire::set(|| {
                PERMIT_REACQUIRES.fetch_add(1, Ordering::SeqCst);
            });
            shmem_seams::shmem_alloc::set(|size| {
                let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
                // Cluster-lifetime allocation, deliberately leaked (C: shmem).
                let p = unsafe { std::alloc::alloc_zeroed(layout) };
                assert!(!p.is_null());
                Ok(p)
            });
            shmem_seams::add_size::set(|a, b| {
                a.checked_add(b)
                    .ok_or_else(|| Box::new(PgError::error("shmem size overflow")))
            });
            shmem_seams::mul_size::set(|a, b| {
                a.checked_mul(b)
                    .ok_or_else(|| Box::new(PgError::error("shmem size overflow")))
            });
            static SHMEM_LOCK: AtomicBool = AtomicBool::new(false);
            shmem_seams::shmem_lock_acquire::set(|| {
                while SHMEM_LOCK.swap(true, Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            });
            shmem_seams::shmem_lock_release::set(|| SHMEM_LOCK.store(false, Ordering::Release));
            smgr_seams::smgr_read::set(|_rlb, _f, blocknum, buffer| {
                SYNC_READS.fetch_add(1, Ordering::Relaxed);
                valid_page_into(buffer, blocknum);
                Ok(())
            });
            // mdstartreadv stand-in: the sync arrival path runs the REAL
            // pgaio pipeline against rel-sync.dat (plain-blkno markers, so
            // sync-vs-uring content assertions keep their meaning).
            smgr_seams::smgr_startreadv::set(|rlb, _f, blocknum, pages| {
                SYNC_READS.fetch_add(1, Ordering::Relaxed);
                let fd = sync_file_fd(blocknum, pages.len() as u32);
                globals::HoldInterrupts();
                let iovcnt = aio_core::pgaio_io_set_iovec_pages(pages, BLCKSZ);
                let ioh = aio_core::pgaio_io_current();
                aio_core::pgaio_io_set_target_smgr(
                    ioh,
                    rlb.locator,
                    ForkNumber::MAIN_FORKNUM,
                    blocknum,
                    pages.len() as u32,
                    false,
                    false,
                );
                aio_core::pgaio_io_register_callbacks(
                    ioh,
                    types_storage::aio::PGAIO_HCB_MD_READV,
                    0,
                );
                let r = aio_core::pgaio_io_start_readv_current(
                    fd,
                    iovcnt,
                    blocknum as i64 * BLCKSZ as i64,
                );
                if r.is_ok() {
                    globals::ResumeInterrupts();
                }
                r
            });
            smgr_seams::aio_md_readv_complete::set(|ioh, prior, _| {
                let mut r = prior;
                if prior.result < 0 {
                    r.status = types_storage::aio::PgAioResultStatus::Error;
                    r.id = types_storage::aio::PGAIO_HCB_MD_READV;
                    r.error_data = (-prior.result) as u32;
                    r.result = 0;
                    return r;
                }
                r.result /= BLCKSZ as i32;
                let nblocks = aio_core::pgaio_io_get_target_data(ioh).smgr.nblocks as i32;
                if r.result == 0 {
                    // C: zero blocks read is a failure (unexpected EOF) —
                    // never surface zero-progress OK to the read retry loop.
                    r.status = types_storage::aio::PgAioResultStatus::Error;
                    r.id = types_storage::aio::PGAIO_HCB_MD_READV;
                    r.error_data = 0;
                } else if r.status != types_storage::aio::PgAioResultStatus::Error
                    && r.result < nblocks
                {
                    r.status = types_storage::aio::PgAioResultStatus::Partial;
                    r.id = types_storage::aio::PGAIO_HCB_MD_READV;
                }
                r
            });
            smgr_seams::aio_md_readv_report::set(|result, _td, elevel| {
                elog::ereport(elevel)
                    .errmsg(format!("test md readv failed: {:?}", result.status))
                    .finish(types_error::ErrorLocation::new("tests", 0, "md_readv_report"))
            });
            smgr_seams::smgr_start_buffer_read::set(|rlb, _f, blocknum, buffer| {
                // URING_REL is the M0 suite's shared relation; the M1
                // tests take fresh relnumbers above it (same file bytes,
                // fresh buffer tags — cold by construction).
                assert!(
                    (URING_REL..URING_REL + 100).contains(&rlb.locator.relNumber),
                    "unexpected test relation {}",
                    rlb.locator.relNumber
                );
                Ok(aio_seams::uring_buf_read::call(
                    uring_file_fd(),
                    blocknum as i64 * BLCKSZ as i64,
                    buffer,
                ))
            });
            s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
            s_lock_seams::finish_spin_delay::set(|_| {});
            ipc_seams::on_shmem_exit::set(|_, _| {});
            ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
            waitevent_seams::pgstat_report_wait_start::set(|_| {});
            waitevent_seams::pgstat_report_wait_end::set(|| {});
            postgres_seams::check_for_interrupts::set(|| Ok(()));
            xact_seams::get_current_transaction_nest_level::set(|| 1);
            pg_sema::init_seams();
            globals::SetIsUnderPostmaster(false);
            globals::SetMaxConnections(TEST_MAX_CONNECTIONS);
            globals::set_max_worker_processes(2);
            globals::SetNBuffers(TEST_NBUFFERS);
            globals::SetMaxBackends(
                TEST_MAX_CONNECTIONS
                    + 3
                    + 2
                    + 2
                    + types_storage::storage::NUM_SPECIAL_WORKER_PROCS,
            );
            lmgr_proc::init_seams();
            lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
                autovacuum_worker_slots: 3,
                max_wal_senders: 2,
                max_prepared_xacts: 2,
                fastpath_lock_groups_per_backend: 1,
            });
            waiteventset::init_seams();
            latch::init_seams();
            lwlock::CreateLWLocks(false).unwrap();
            bufmgr::BufferManagerShmemInit().unwrap();
            bufmgr::init_seams();
            aio_core::init_seams();
            guc_tables::vars::io_max_combine_limit.install_if_absent(
                guc_tables::GucVarAccessors { get: || 16, set: |_| {} },
            );
            aio_core::AioShmemSize().unwrap();
            aio_core::AioShmemInit().unwrap();
            crate::init_seams();
        });
        become_backend();
        guard
    }

    fn uring_smgr() -> RelFileLocatorBackend {
        rel_smgr(URING_REL)
    }

    fn rel_smgr(rel: u32) -> RelFileLocatorBackend {
        RelFileLocatorBackend {
            locator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: rel },
            backend: INVALID_PROC_NUMBER,
        }
    }

    /// A relation nobody has touched: fresh buffer tags over the SAME test
    /// file bytes — every block is cold by construction, so the M1 tests
    /// can assert `Issued` regardless of suite order.
    fn fresh_rel() -> u32 {
        static NEXT: AtomicI32 = AtomicI32::new(1);
        URING_REL + NEXT.fetch_add(1, Ordering::Relaxed) as u32
    }

    fn uring_start_rel(rel: u32, blk: u32) -> Option<PrefetchOutcome> {
        bufmgr::uring_start_read(
            rel_smgr(rel),
            RELPERSISTENCE_PERMANENT,
            ForkNumber::MAIN_FORKNUM,
            blk,
        )
        .unwrap()
    }

    fn read_blk_rel(rel: u32, blk: u32) -> Buffer {
        ReadBufferWithoutRelcache(
            rel_smgr(rel).locator,
            ForkNumber::MAIN_FORKNUM,
            blk,
            ReadBufferMode::Normal,
            None,
            true,
        )
        .unwrap()
    }

    fn uring_start(blk: u32) -> Option<PrefetchOutcome> {
        bufmgr::uring_start_read(
            uring_smgr(),
            RELPERSISTENCE_PERMANENT,
            ForkNumber::MAIN_FORKNUM,
            blk,
        )
        .unwrap()
    }

    fn read_blk(blk: u32) -> Buffer {
        ReadBufferWithoutRelcache(
            uring_smgr().locator,
            ForkNumber::MAIN_FORKNUM,
            blk,
            ReadBufferMode::Normal,
            None,
            true,
        )
        .unwrap()
    }

    fn page_block_field(b: Buffer) -> u32 {
        let p = bufmgr::BufferGetBlockPtr(b);
        // SAFETY: pinned valid buffer in the test.
        let s = unsafe { core::slice::from_raw_parts(p, BLCKSZ) };
        u32::from_ne_bytes(s[24..28].try_into().unwrap())
    }

    fn uring_here() -> bool {
        if aio_seams::uring_available::call() {
            return true;
        }
        eprintln!("io_uring unavailable here; skipping");
        false
    }

    // The O_DIRECT probe: on the fleet NVMe (/work) DIO MUST engage, so a
    // buffered fallback there is a loud failure, not a silent skip.
    #[test]
    fn dio_engages_on_real_filesystem() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        uring_file_fd();
        if std::path::Path::new("/work").is_dir() {
            assert!(DIO_ENGAGED.load(Ordering::Relaxed), "O_DIRECT refused on /work NVMe");
        } else if !DIO_ENGAGED.load(Ordering::Relaxed) {
            eprintln!("O_DIRECT unavailable on this filesystem; suites ran buffered");
        }
    }

    #[test]
    fn prefetch_lands_in_pool_and_arrival_hits() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        let before_sync = SYNC_READS.load(Ordering::Relaxed);
        for blk in 0..4u32 {
            assert_eq!(uring_start(blk), Some(PrefetchOutcome::Issued), "block {blk}");
        }
        assert_eq!(uring_start(0), Some(PrefetchOutcome::Cached));
        let mut bufs = Vec::new();
        for blk in 0..4u32 {
            let b = read_blk(blk);
            assert_eq!(page_block_field(b), blk + 100, "page must arrive via uring DMA");
            ReleaseBuffer(b).unwrap();
            bufs.push(b);
        }
        assert_eq!(SYNC_READS.load(Ordering::Relaxed), before_sync, "no sync fallback");
        bufmgr::uring_collect_pins();
        for b in bufs {
            assert_eq!(GetPrivateRefCount(b), 0, "prefetch pin must be collected");
        }
        AtEOXact_Buffers(true);
    }

    #[test]
    fn short_read_degrades_to_sync_arrival() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        let blk = FILE_PAGES + 50;
        let before_sync = SYNC_READS.load(Ordering::Relaxed);
        assert_eq!(uring_start(blk), Some(PrefetchOutcome::Issued));
        let b = read_blk(blk);
        assert_eq!(page_block_field(b), blk, "content must come from the sync re-read");
        assert_eq!(SYNC_READS.load(Ordering::Relaxed), before_sync + 1);
        ReleaseBuffer(b).unwrap();
        bufmgr::uring_collect_pins();
        assert_eq!(GetPrivateRefCount(b), 0);
        AtEOXact_Buffers(true);
    }

    #[test]
    fn foreign_thread_completes_issuers_io() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        let blk = 5u32;
        let before_sync = SYNC_READS.load(Ordering::Relaxed);
        assert_eq!(uring_start(blk), Some(PrefetchOutcome::Issued));
        let t = std::thread::spawn(move || {
            become_backend();
            let b = read_blk(blk);
            let field = page_block_field(b);
            ReleaseBuffer(b).unwrap();
            field
        });
        assert_eq!(t.join().unwrap(), blk + 100);
        assert_eq!(
            SYNC_READS.load(Ordering::Relaxed),
            before_sync,
            "foreign thread must drain the issuer's ring, not re-read"
        );
        AtEOXact_Buffers(true);
    }

    #[test]
    fn eoxact_drains_unread_prefetches() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        for blk in 6..FILE_PAGES {
            assert_eq!(uring_start(blk), Some(PrefetchOutcome::Issued), "block {blk}");
        }
        AtEOXact_Buffers(true);
        for blk in 6..FILE_PAGES {
            let b = read_blk(blk);
            assert_eq!(page_block_field(b), blk + 100);
            assert_eq!(GetPrivateRefCount(b), 1, "only the arrival pin may remain");
            ReleaseBuffer(b).unwrap();
        }
        AtEOXact_Buffers(true);
    }

    // ---- M1 §2.9: IoToken wiring, boundary reaping, permit discipline ------

    fn permit_counts() -> (i32, i32) {
        (PERMIT_RELEASES.load(Ordering::SeqCst), PERMIT_REACQUIRES.load(Ordering::SeqCst))
    }

    /// Every wait path pairs release with reacquire — the IoGuard contract.
    fn assert_permit_pairing(before: (i32, i32)) {
        let after = permit_counts();
        assert_eq!(
            after.0 - before.0,
            after.1 - before.1,
            "io_permit_release must pair 1:1 with io_permit_reacquire"
        );
    }

    /// Worker-ring lifecycle + owner boundary reap driving a foreign
    /// waiter's arrival: the issuer marks its ring boundary-reaped (the
    /// runtime worker shape), a foreign thread arrives at the pending read
    /// (token park when it loses the race to the CQE, settled peek when it
    /// wins — both legal), and the issuer's boundary reaps complete the IO
    /// and drop the issuer pin. No sync fallback, byte parity, permits
    /// paired.
    #[test]
    fn boundary_reap_unparks_foreign_waiter_on_pool_ring() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        let ring = aio_seams::uring_worker_ring_init::call();
        assert!(ring >= 0, "worker ring init must succeed where uring is available");
        let before_sync = SYNC_READS.load(Ordering::Relaxed);
        let before_permits = permit_counts();

        let rel = fresh_rel();
        let blk = 1u32;
        assert_eq!(uring_start_rel(rel, blk), Some(PrefetchOutcome::Issued));

        let foreign = std::thread::spawn(move || {
            become_backend();
            let b = read_blk_rel(rel, blk);
            let field = page_block_field(b);
            ReleaseBuffer(b).unwrap();
            field
        });
        // The issuer's task-boundary duty, on its own cadence.
        while !foreign.is_finished() {
            aio_seams::uring_boundary_reap::call();
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        assert_eq!(foreign.join().unwrap(), blk + 100, "page must arrive via uring DMA");
        assert_eq!(SYNC_READS.load(Ordering::Relaxed), before_sync, "no sync fallback");
        assert_permit_pairing(before_permits);

        // Boundary reaps must have collected the issuer pin.
        aio_seams::uring_boundary_reap::call();
        let b = read_blk_rel(rel, blk);
        assert_eq!(GetPrivateRefCount(b), 1, "issuer prefetch pin must be collected");
        ReleaseBuffer(b).unwrap();
        AtEOXact_Buffers(true);
    }

    /// Peek-complete elision: a wait that finds the IO already settled at
    /// the peek must not touch the permit seams. The CQE is given ample
    /// time to arrive before the waiter shows up; if the device is
    /// pathologically slow the wait legitimately blocks (permits paired) —
    /// the elision assertion is then skipped rather than failed.
    #[test]
    fn settled_peek_elides_permit_release() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        let rel = fresh_rel();
        let blk = 2u32;
        assert_eq!(uring_start_rel(rel, blk), Some(PrefetchOutcome::Issued));
        // Ample time for the µs-scale DMA to land its CQE (unreaped).
        std::thread::sleep(std::time::Duration::from_millis(100));
        let before_permits = permit_counts();
        let t = std::thread::spawn(move || {
            become_backend();
            let b = read_blk_rel(rel, blk);
            let field = page_block_field(b);
            ReleaseBuffer(b).unwrap();
            field
        });
        assert_eq!(t.join().unwrap(), blk + 100);
        let after = permit_counts();
        assert_permit_pairing(before_permits);
        if after.0 != before_permits.0 {
            eprintln!("CQE lost the 100ms race; elision not exercised this run");
        } else {
            assert_eq!(after, before_permits, "settled peek must not churn permits");
        }
        bufmgr::uring_collect_pins();
        AtEOXact_Buffers(true);
    }

    /// Fault injection (drop-CQE hook): every IoToken completion is dropped
    /// at the reap site, so a token-parked waiter never gets its wake. The
    /// recheck backstop (50ms cadence in this binary) must recover via the
    /// settled-state probe / degraded reap — the read completes correctly
    /// and nothing hangs.
    #[test]
    fn dropped_token_completion_recovered_by_recheck_backstop() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                crate::test_set_drop_token_complete_1in(0);
            }
        }
        let _reset = ResetHook;
        crate::test_set_drop_token_complete_1in(1); // drop EVERY token wake

        let ring = aio_seams::uring_worker_ring_init::call();
        assert!(ring >= 0);
        let before_sync = SYNC_READS.load(Ordering::Relaxed);
        let before_permits = permit_counts();
        let rel = fresh_rel();
        let blk = 3u32;
        assert_eq!(uring_start_rel(rel, blk), Some(PrefetchOutcome::Issued));

        let foreign = std::thread::spawn(move || {
            become_backend();
            let b = read_blk_rel(rel, blk);
            let field = page_block_field(b);
            ReleaseBuffer(b).unwrap();
            field
        });
        // Owner boundary reaps: consume the CQE (state settles, pin
        // collects) but the token wake is dropped by the hook.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !foreign.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "waiter failed to recover from a dropped token completion"
            );
            aio_seams::uring_boundary_reap::call();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(foreign.join().unwrap(), blk + 100);
        assert_eq!(SYNC_READS.load(Ordering::Relaxed), before_sync, "no sync fallback");
        assert_permit_pairing(before_permits);
        bufmgr::uring_collect_pins();
        AtEOXact_Buffers(true);
    }

    /// The targeted M1 e2e at unit altitude: a COLD scan (fresh buffers, all
    /// reads via uring DMA) at DOP — one boundary-reaping issuer ring (the
    /// pool-worker shape) racing several foreign arrival threads over every
    /// block, byte-parity against the known page images, permit pairing
    /// intact, zero sync fallbacks. (The server-level cold heap scan at DOP
    /// through runtime scan TaskSets lands with M1 lane A — this exercises
    /// the full ring/reap/IoToken/IoGuard stack under real concurrency.)
    #[test]
    fn cold_scan_at_dop_byte_parity() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        // Fresh relation: every block cold by construction.
        let rel = fresh_rel();
        let ring = aio_seams::uring_worker_ring_init::call();
        assert!(ring >= 0);
        let before_sync = SYNC_READS.load(Ordering::Relaxed);
        let before_permits = permit_counts();

        // Issue the whole file's reads from the issuer (readahead shape).
        for blk in 0..FILE_PAGES {
            assert_eq!(uring_start_rel(rel, blk), Some(PrefetchOutcome::Issued), "block {blk}");
        }
        // DOP arrival threads, each scanning EVERY block (max contention on
        // the WaitIO/token paths), asserting byte parity per page.
        const DOP: usize = 4;
        let workers: Vec<_> = (0..DOP)
            .map(|w| {
                std::thread::spawn(move || {
                    become_backend();
                    for step in 0..FILE_PAGES {
                        // Stagger start blocks so threads collide on
                        // different in-flight IOs.
                        let blk = (step + w as u32 * 2) % FILE_PAGES;
                        let b = read_blk_rel(rel, blk);
                        assert_eq!(
                            page_block_field(b),
                            blk + 100,
                            "worker {w}: page {blk} must arrive via uring DMA"
                        );
                        ReleaseBuffer(b).unwrap();
                    }
                })
            })
            .collect();
        // The issuer's boundary-reap cadence runs the whole time; finished
        // workers are JOINED so their assertion panics surface here.
        let mut pending = workers;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !pending.is_empty() {
            assert!(std::time::Instant::now() < deadline, "cold DOP scan wedged");
            aio_seams::uring_boundary_reap::call();
            std::thread::sleep(std::time::Duration::from_micros(200));
            let (done, rest): (Vec<_>, Vec<_>) =
                pending.into_iter().partition(|h| h.is_finished());
            for h in done {
                h.join().unwrap();
            }
            pending = rest;
        }
        assert_eq!(
            SYNC_READS.load(Ordering::Relaxed),
            before_sync,
            "cold scan must be served entirely by uring DMA"
        );
        assert_permit_pairing(before_permits);
        // Every issuer pin collected; nothing stranded.
        aio_seams::uring_boundary_reap::call();
        for blk in 0..FILE_PAGES {
            let b = read_blk_rel(rel, blk);
            assert_eq!(GetPrivateRefCount(b), 1, "block {blk}: stranded issuer pin");
            ReleaseBuffer(b).unwrap();
        }
        AtEOXact_Buffers(true);
    }

    /// Worker-ring teardown at pool exit: in-flight DMA is waited out
    /// (tokens complete), the ring dies, and later reads fall back cleanly.
    #[test]
    fn worker_ring_teardown_waits_out_inflight() {
        let _g = setup();
        if !uring_here() {
            return;
        }
        // A dedicated "pool worker" thread with its own ring (TLS-keyed).
        let rel = fresh_rel();
        let worker = std::thread::spawn(move || {
            become_backend();
            let ring = aio_seams::uring_worker_ring_init::call();
            assert!(ring >= 0);
            let blk = 4u32;
            assert_eq!(uring_start_rel(rel, blk), Some(PrefetchOutcome::Issued));
            // Exit with the IO in flight: teardown must wait it out, run
            // its completion (BM_VALID or BM_IO_ERROR — never a torn page)
            // and collect the slot pin before the ring dies.
            aio_seams::uring_worker_ring_teardown::call();
            blk
        });
        let blk = worker.join().unwrap();
        // Arrival on this thread: the completed page (or a clean sync
        // re-read on BM_IO_ERROR) — content must be correct either way.
        let before_permits = permit_counts();
        let b = read_blk_rel(rel, blk);
        let field = page_block_field(b);
        assert!(field == blk + 100 || field == blk, "torn/foreign page after teardown");
        ReleaseBuffer(b).unwrap();
        assert_permit_pairing(before_permits);
        AtEOXact_Buffers(true);
    }
}
