//! io_uring reads landing directly in shared-buffer pool pages
//! (method_io_uring.c + the StartReadBuffers read subset, collapsed to the
//! thread-per-backend model): bufmgr pins the victim and sets
//! BM_IO_IN_PROGRESS, we submit the SQE (wref armed first), and ANY thread may
//! drain a ring's completions (C's deadlock rule: whoever waits completes).
//! Divergence from C 18: availability-gated, not io_method-gated; fadvise
//! stays the fallback where the ring is absent.

pub fn init_seams() {
    aio_seams::uring_buf_read::set(uring_buf_read);
    aio_seams::uring_buf_read_wait::set(uring_buf_read_wait);
    aio_seams::uring_collect_done::set(uring_collect_done);
    aio_seams::uring_drain_own::set(uring_drain_own);
    aio_seams::uring_available::set(uring_available);
    aio_seams::uring_drain_all_raw::set(uring_drain_all_raw);
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
}

use imp::*;

#[cfg(target_os = "linux")]
mod imp {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, Ordering};
    use std::sync::{Mutex, MutexGuard};

    use elog::ereport;
    use types_error::{ErrorLocation, LOG};

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

    #[derive(Clone, Copy)]
    struct Slot {
        buffer: i32,
        gen: u64,
    }

    struct RingState {
        alive: bool,
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
                .finish(ErrorLocation::new("method_io_uring.c", 0, "uring_buf_read"));
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
                slots: [Slot { buffer: 0, gen: 0 }; SLOTS as usize],
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
        let slot = st.free.trailing_zeros();
        let gen = st.next_gen;
        st.next_gen += 1;
        st.slots[slot as usize] = Slot { buffer, gen };
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
            log_fallback("io_uring_enter", e);
            teardown(&mut st);
            return false;
        }
        st.free &= !(1u128 << slot);
        st.inflight += 1;
        true
    }

    pub fn uring_buf_read_wait(aio_index: u32, generation: u64) {
        if aio_index == 0 {
            return;
        }
        let idx = aio_index - 1;
        let (ring_id, slot) = ((idx / SLOTS) as usize, idx % SLOTS);
        let p = REGISTRY[ring_id].load(Ordering::Acquire);
        if p.is_null() {
            return;
        }
        // SAFETY: registry entries are leaked, never freed.
        let mut st = lock(unsafe { &*p });
        let bit = 1u128 << slot;
        loop {
            if !st.alive
                || st.slots[slot as usize].gen != generation
                || st.free & bit != 0
                || st.done & bit != 0
            {
                return;
            }
            reap_locked(&mut st);
            if st.done & bit != 0 {
                return;
            }
            wait_locked(&mut st);
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
