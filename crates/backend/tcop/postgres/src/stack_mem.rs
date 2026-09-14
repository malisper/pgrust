//! Wave-4 floor work (docs/design/connection-scaling.md): the backend
//! thread's stack as a memory citizen.
//!
//! Two jobs:
//! 1. **Census** ([`stack_census`], surfaced by `pgrust: memctx`): where the
//!    stack region is, how deep we are right now, and how many of its pages
//!    have ever been dirtied (the phys_footprint the bootstrap dip left
//!    behind — the D3.0/wave-4 floor census line).
//! 2. **Release on passivation** ([`release_idle_stack`]): at idle
//!    passivation, madvise away the dirty pages BELOW the current stack
//!    frame so a parked connection stops paying for the deepest excursion
//!    it ever made (backend bootstrap and catalog-heavy queries dip
//!    1-2MB deep; the idle wait loop sits within ~100KB of the stack top).
//!
//! # Safety argument for releasing stack pages under a live thread
//!
//! The released range is computed from the **current frame address**, never
//! from configuration: `[region_low, frame_addr - RED_ZONE)`, page-aligned
//! inward. Why no live frame can exist in that range:
//!
//! - Stacks grow downward on every supported target (aarch64, x86_64). All
//!   live frames of this thread occupy `[SP, region_high)`; everything below
//!   SP is dead by the ABI (no callee-owned data below SP beyond the
//!   128-byte x86-64 red zone, which RED_ZONE dwarfs). `frame_addr` (the
//!   address of a local in the caller's frame) is >= SP, and the release
//!   cutoff sits RED_ZONE below it — pages at or above the cutoff are
//!   untouched, so the gap between `frame_addr` and the true SP (one small
//!   frame, well under 4KB) is covered many times over.
//! - The releasing thread IS the passivating thread, parked at a known
//!   shallow depth (ReadCommand → ProcessClientReadInterrupt →
//!   ProcessInterrupts → IdlePassivate → here); nothing else executes on
//!   this stack concurrently — with one exception: an async signal could be
//!   delivered to this thread mid-madvise. Its frame lands just below the
//!   interrupted SP, i.e. inside the preserved RED_ZONE (our process-signal
//!   handlers touch only atomics + SetLatch, a few KB at most; the crash
//!   handlers are SA_ONSTACK and run on the sigaltstack). RED_ZONE = 192KB
//!   absorbs any such frame with two orders of magnitude to spare.
//! - The madvise flavors used never unmap: a later (deeper) call chain that
//!   re-enters the released range takes a zero-fill refault and writes its
//!   frame before reading it — stack memory is never legitimately read
//!   before write, so zero-filled pages are indistinguishable from the
//!   never-touched pages a fresh thread starts with.
//!
//! # Flavor choice
//!
//! - **Linux: MADV_DONTNEED.** The pages leave RSS *and* the cgroup/memcg
//!   charge immediately — the number operators watch on the (Linux) CI cluster.
//!   MADV_FREE would be a cheaper syscall but the pages stay charged until
//!   global reclaim runs, so 500 passivated connections would look exactly
//!   as fat as before on any container metric; committed-memory accounting
//!   (Committed_AS) is unaffected either way since the mapping survives.
//!   The refault cost (a zero-fill minor fault per re-touched page, ~µs) is
//!   paid only on reactivation and only for pages the next query actually
//!   dips into — measured in the wave-4 notes.
//! - **macOS: MADV_FREE_REUSABLE**, the only flavor that moves
//!   phys_footprint (the metric this campaign measures on the dev box;
//!   mimalloc uses the same flavor for its own retention). Reactivation
//!   madvises MADV_FREE_REUSE over the same range ([`reuse_idle_stack`],
//!   called at next-command arrival) so re-dirtied pages are charged to the
//!   process again — without it the *warmed* footprint of a reactivated
//!   backend would under-count, faking the very number this work reports.
//! - Other targets: no-op.
//!
//! Kill switches: GUC `idle_passivate_stack` (PGC_SIGHUP, default on) under
//! the existing `idle_passivate_timeout` gate; PGRUST_PASSIVATE_STACK env
//! (0 = off) wins when set (harness precedent: PGRUST_IDLE_PASSIVATE_SECS).

#![allow(dead_code)]

/// Preserved band below the current frame address. Must dominate: the
/// frame-addr-vs-SP slack (<4KB), an async signal frame + handler stack
/// (<16KB), and the deepest call made between release and returning to the
/// wait loop (elog DEBUG1 formatting; <32KB). 192KB = 48 pages of margin.
const RED_ZONE: usize = 192 * 1024;

#[derive(Clone, Copy, Debug, Default)]
pub struct StackCensus {
    /// Usable stack region (guard excluded), bytes.
    pub region_bytes: usize,
    /// Current depth: region_high - frame_addr.
    pub depth_now: usize,
    /// Pages of the region currently resident (mincore incore).
    pub resident_bytes: usize,
    /// Pages ever dirtied and still charged (macOS: modified | paged-out;
    /// Linux mincore only reports residency, so this equals resident_bytes
    /// there — stack pages are anonymous-private, resident ≡ dirtied).
    pub dirty_bytes: usize,
    /// High-water: region_high - lowest charged page address, i.e. the
    /// deepest excursion still visible in memory accounting.
    pub high_water: usize,
}

/// Usable stack bounds of the calling thread, `(low, high)`, guard pages
/// excluded. None when the platform probe fails.
#[cfg(target_os = "macos")]
fn stack_region() -> Option<(usize, usize)> {
    // SAFETY: pthread_self is the live current thread; the *_np getters are
    // pure reads. On macOS stackaddr is the HIGH end and the guard sits
    // below stackaddr - stacksize, outside the reported range.
    unsafe {
        let t = libc::pthread_self();
        let high = libc::pthread_get_stackaddr_np(t) as usize;
        let size = libc::pthread_get_stacksize_np(t);
        if high == 0 || size == 0 {
            return None;
        }
        Some((high - size, high))
    }
}

#[cfg(target_os = "linux")]
fn stack_region() -> Option<(usize, usize)> {
    // SAFETY: standard pthread_getattr_np(self) probe; attr is destroyed on
    // every path. NPTL includes the guard area at the LOW end of the
    // reported stack for pthread-created threads — skip it explicitly.
    unsafe {
        let mut attr: libc::pthread_attr_t = core::mem::zeroed();
        if libc::pthread_getattr_np(libc::pthread_self(), &mut attr) != 0 {
            return None;
        }
        let mut lo: *mut libc::c_void = core::ptr::null_mut();
        let mut size: libc::size_t = 0;
        let mut guard: libc::size_t = 0;
        let ok = libc::pthread_attr_getstack(&attr, &mut lo, &mut size) == 0;
        let _ = libc::pthread_attr_getguardsize(&attr, &mut guard);
        libc::pthread_attr_destroy(&mut attr);
        if !ok || lo.is_null() || size == 0 || size <= guard {
            return None;
        }
        Some((lo as usize + guard, lo as usize + size))
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn stack_region() -> Option<(usize, usize)> {
    None
}

fn page_size() -> usize {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        // SAFETY: sysconf(_SC_PAGESIZE) is a pure read.
        let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if p > 0 {
            return p as usize;
        }
    }
    4096
}

/// A conservative proxy for the caller's SP: the address of a local in the
/// CALLER's frame (passed in so inlining can't hoist it above deeper
/// frames). Always >= the true SP of any code that runs after the caller.
#[inline(always)]
fn frame_addr(marker: &u8) -> usize {
    marker as *const u8 as usize
}

// macOS mincore vec bits (sys/mman.h).
#[cfg(target_os = "macos")]
mod mac_mincore {
    pub const INCORE: u8 = 0x1;
    pub const MODIFIED: u8 = 0x4;
    pub const MODIFIED_OTHER: u8 = 0x10;
    pub const PAGED_OUT: u8 = 0x20;
}

/// Residency scan of this thread's stack region. Read-only; used by the
/// `pgrust: memctx` census. None when bounds or mincore are unavailable.
pub fn stack_census() -> Option<StackCensus> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        stack_census_impl()
    }
    // No mincore (wasm32-wasip1 and the rest): stack_region() has no arm
    // there either — the census is simply unavailable.
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn stack_census_impl() -> Option<StackCensus> {
    let marker = 0u8;
    let (lo, hi) = stack_region()?;
    let page = page_size();
    let start = lo & !(page - 1);
    let npages = (hi - start) / page;
    if npages == 0 || npages > 1 << 22 {
        return None;
    }
    let mut vec = vec![0u8; npages];
    // SAFETY: [start, start+npages*page) lies within this thread's mapped
    // stack region; vec is npages bytes as mincore requires.
    let rc = unsafe {
        libc::mincore(start as *mut libc::c_void, npages * page, vec.as_mut_ptr().cast())
    };
    if rc != 0 {
        return None;
    }
    let mut resident = 0usize;
    let mut dirty = 0usize;
    let mut lowest_charged = hi;
    for (i, &b) in vec.iter().enumerate() {
        #[cfg(target_os = "macos")]
        let (is_res, is_dirty) = (
            b & mac_mincore::INCORE != 0,
            b & (mac_mincore::MODIFIED | mac_mincore::MODIFIED_OTHER | mac_mincore::PAGED_OUT)
                != 0,
        );
        #[cfg(not(target_os = "macos"))]
        let (is_res, is_dirty) = (b & 1 != 0, b & 1 != 0);
        if is_res {
            resident += page;
        }
        if is_dirty || is_res {
            let addr = start + i * page;
            if addr < lowest_charged {
                lowest_charged = addr;
            }
        }
        if is_dirty {
            dirty += page;
        }
    }
    Some(StackCensus {
        region_bytes: hi - lo,
        depth_now: hi.saturating_sub(frame_addr(&marker)),
        resident_bytes: resident,
        dirty_bytes: dirty,
        high_water: hi.saturating_sub(lowest_charged),
    })
}

/// Env/GUC gate: PGRUST_PASSIVATE_STACK (0=off) wins; else the
/// `idle_passivate_stack` GUC (default on).
fn stack_release_enabled() -> bool {
    static ENV: std::sync::OnceLock<Option<bool>> = std::sync::OnceLock::new();
    if let Some(v) = *ENV.get_or_init(|| {
        std::env::var("PGRUST_PASSIVATE_STACK")
            .ok()
            .map(|v| v.trim() != "0")
    }) {
        return v;
    }
    guc_tables::backing::idle_passivate_stack()
}

thread_local! {
    // macOS: the range a passivation marked REUSABLE, pending a REUSE at
    // reactivation so re-dirtied pages are charged again (see module doc).
    static RELEASED: core::cell::Cell<Option<(usize, usize)>> =
        const { core::cell::Cell::new(None) };
}

/// Release this thread's dead stack pages (idle passivation only — see the
/// module safety argument). Returns bytes madvised, 0 when disabled,
/// unsupported, or nothing to release.
pub fn release_idle_stack() -> usize {
    if !stack_release_enabled() {
        return 0;
    }
    let marker = 0u8;
    let Some((lo, _hi)) = stack_region() else { return 0 };
    let page = page_size();
    let start = (lo + page - 1) & !(page - 1);
    let cutoff = frame_addr(&marker).saturating_sub(RED_ZONE) & !(page - 1);
    if cutoff <= start {
        return 0;
    }
    let len = cutoff - start;
    #[cfg(target_os = "linux")]
    {
        // MADV_DONTNEED — see the module doc's flavor choice. Errors are
        // ignored: advisory call on our own mapping; worst case the pages
        // simply stay charged.
        // SAFETY: [start, cutoff) is entirely below the preserved red zone of
        // this thread's own stack (module safety argument).
        unsafe {
            libc::madvise(start as *mut libc::c_void, len, libc::MADV_DONTNEED);
        }
        return len;
    }
    #[cfg(target_os = "macos")]
    {
        const MADV_FREE_REUSABLE: libc::c_int = 7;
        // SAFETY: as above; MADV_FREE_REUSABLE never unmaps.
        let rc = unsafe {
            libc::madvise(start as *mut libc::c_void, len, MADV_FREE_REUSABLE)
        };
        if rc == 0 {
            RELEASED.with(|c| {
                let merged = match c.get() {
                    Some((s0, e0)) => (s0.min(start), e0.max(cutoff)),
                    None => (start, cutoff),
                };
                c.set(Some(merged));
            });
            return len;
        }
        return 0;
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = len;
        0
    }
}

/// macOS reactivation half: mark the previously released range REUSE so
/// re-dirtied pages count against phys_footprint again. Cheap no-op
/// everywhere else and when no release is pending. Called at next-command
/// arrival (main_loop's passivation disarm point).
pub fn reuse_idle_stack() {
    #[cfg(target_os = "macos")]
    {
        if let Some((start, end)) = RELEASED.with(|c| c.take()) {
            const MADV_FREE_REUSE: libc::c_int = 8;
            // SAFETY: same range a prior release_idle_stack madvised on this
            // same thread; purely an accounting advisory.
            unsafe {
                libc::madvise(
                    start as *mut libc::c_void,
                    end - start,
                    MADV_FREE_REUSE,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[inline(never)]
    fn dip(n: usize) -> u64 {
        let mut buf = [0u8; 64 * 1024];
        buf[0] = n as u8;
        buf[32 * 1024] = n as u8;
        buf[63 * 1024] = (n >> 8) as u8;
        let buf = std::hint::black_box(&mut buf);
        let s = buf[0] as u64 + buf[32 * 1024] as u64 + buf[63 * 1024] as u64;
        if n == 0 { s } else { s + dip(n - 1) }
    }

    #[test]
    fn census_sees_a_deep_excursion() {
        if !cfg!(any(target_os = "macos", target_os = "linux")) {
            return;
        }
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let _ = dip(30); // ~2MB excursion dirties pages
                let c = stack_census().expect("stack census on supported platform");
                assert!(c.region_bytes > 0);
                assert!(c.depth_now > 0 && c.depth_now <= c.region_bytes);
                // The excursion dirtied real pages, and the deepest one is
                // beyond 1MB from the top.
                assert!(c.dirty_bytes > 0, "no dirty pages after a 2MB dip: {c:?}");
                assert!(c.high_water >= 1024 * 1024, "high water too shallow: {c:?}");
            })
            .expect("spawn")
            .join()
            .expect("join");
    }

    #[test]
    fn release_then_deep_touch_survives() {
        // The whole point: release dead pages, then dip deep again and make
        // sure the refaulted stack behaves like a fresh one.
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let deep0 = dip(40); // ~2.5MB excursion
                // Force the release path regardless of GUC default state.
                std::env::set_var("PGRUST_PASSIVATE_STACK", "1");
                let released = release_idle_stack();
                if cfg!(any(target_os = "macos", target_os = "linux")) {
                    assert!(released > 0, "expected a nonzero release");
                }
                reuse_idle_stack();
                let deep1 = dip(40);
                assert_eq!(deep0, deep1);
            })
            .expect("spawn")
            .join()
            .expect("join");
    }
}
