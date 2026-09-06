// mimalloc is a C allocator that does not build for wasm32; Rust's
// wasm32-wasip1 std defaults to dlmalloc, so the wasm arm simply takes the
// std default allocator and the release hook stays unset (mcx release is a
// no-op without a hook). Native arm unchanged.
//
// Debug builds wrap the allocator in an env-gated leak tracker (FPBUDGET-1
// instrumentation, `PGRUST_ALLOC_TRACK=1`): allocations made on backend
// threads are recorded with a frame-pointer backtrace and surviving records
// are dumped at process exit for offline symbolication (atos). Release/dist
// builds compile the bare allocator — zero added cost.
#[cfg(all(not(target_family = "wasm"), not(debug_assertions)))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(not(target_family = "wasm"), debug_assertions))]
#[global_allocator]
static GLOBAL: alloc_track::Tracker = alloc_track::Tracker(mimalloc::MiMalloc);

#[cfg(all(not(target_family = "wasm"), debug_assertions))]
mod alloc_track {
    use std::alloc::{GlobalAlloc, Layout};
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    use std::sync::Mutex;

    pub static ENABLED: AtomicBool = AtomicBool::new(false);
    // PGRUST_ALLOC_TRACK=all: record every thread, not just pg:backend*
    // (runtime pool / lane workers allocate on their own threads).
    pub static ALL_THREADS: AtomicBool = AtomicBool::new(false);
    // SIGWINCH (PGRUST_ALLOC_TRACK=...,dump) requests a mid-run dump of the
    // live set; the next tracked allocation performs it under the lock, so
    // the query state is still live (an exit-time dump only sees what
    // survives the exiting thread's unwind).
    pub static DUMP_REQ: AtomicBool = AtomicBool::new(false);
    pub extern "C" fn request_dump(_sig: libc::c_int) {
        DUMP_REQ.store(true, Relaxed);
    }

    const BT_DEPTH: usize = 28;

    struct Rec {
        tname: &'static str,
        size: usize,
        bt: [usize; BT_DEPTH],
        n: usize,
    }

    static LIVE: Mutex<Option<HashMap<usize, Rec>>> = Mutex::new(None);

    thread_local! {
        static IN_HOOK: Cell<bool> = const { Cell::new(false) };
        // Resolved once per thread: only backend threads are tracked.
        // (tracked: 0 unknown, 1 no, 2 yes; name: interned on first use). One
        // slot: the session TLS census is pinned, so no second thread_local.
        static TRACKED: Cell<(u8, Option<&'static str>)> = const { Cell::new((0, None)) };
    }

    fn thread_name() -> &'static str {
        TRACKED.with(|t| {
            let (tracked, name) = t.get();
            if let Some(n) = name {
                return n;
            }
            let n: &'static str = Box::leak(
                std::thread::current().name().unwrap_or("<unnamed>").to_owned().into_boxed_str(),
            );
            t.set((tracked, Some(n)));
            n
        })
    }

    #[inline]
    fn thread_tracked() -> bool {
        match TRACKED.get() {
            (2, _) => true,
            (1, _) => false,
            (_, name) => {
                let is_backend = ALL_THREADS.load(Relaxed)
                    || std::thread::current()
                        .name()
                        .is_some_and(|n| n.starts_with("pg:backend"));
                TRACKED.set((if is_backend { 2 } else { 1 }, name));
                is_backend
            }
        }
    }

    // aarch64 frame-pointer walk (debug builds keep x29 chains intact).
    #[cfg(target_arch = "aarch64")]
    unsafe fn backtrace_fp(buf: &mut [usize; BT_DEPTH]) -> usize {
        let mut fp: usize;
        core::arch::asm!("mov {}, x29", out(reg) fp);
        let mut n = 0;
        let mut prev = 0usize;
        while n < BT_DEPTH
            && fp > prev
            && fp % 8 == 0
            && (prev == 0 || fp - prev < (64 << 20))
        {
            let lr = *((fp + 8) as *const usize);
            if lr < 0x1_0000 {
                break;
            }
            buf[n] = lr;
            n += 1;
            prev = fp;
            fp = *(fp as *const usize);
        }
        n
    }

    #[cfg(not(target_arch = "aarch64"))]
    unsafe fn backtrace_fp(_buf: &mut [usize; BT_DEPTH]) -> usize {
        0
    }

    fn record_alloc(p: *mut u8, l: Layout) {
        if p.is_null() || !ENABLED.load(Relaxed) {
            return;
        }
        IN_HOOK.with(|h| {
            if h.get() {
                return;
            }
            h.set(true);
            if thread_tracked() {
                let mut bt = [0usize; BT_DEPTH];
                // SAFETY: fp walk with monotonic + alignment guards.
                let n = unsafe { backtrace_fp(&mut bt) };
                let mut g = LIVE.lock().unwrap_or_else(|e| e.into_inner());
                g.get_or_insert_with(HashMap::new)
                    .insert(p as usize, Rec { tname: thread_name(), size: l.size(), bt, n });
                if DUMP_REQ.swap(false, Relaxed) {
                    if let Some(m) = g.as_ref() {
                        dump_map(m, "mid-run");
                    }
                }
            }
            h.set(false);
        });
    }

    fn record_free(p: *mut u8) {
        if p.is_null() || !ENABLED.load(Relaxed) {
            return;
        }
        IN_HOOK.with(|h| {
            if h.get() {
                return;
            }
            h.set(true);
            let mut g = LIVE.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(m) = g.as_mut() {
                m.remove(&(p as usize));
            }
            h.set(false);
        });
    }

    pub extern "C" fn dump() {
        ENABLED.store(false, Relaxed);
        let g = LIVE.lock().unwrap_or_else(|e| e.into_inner());
        let Some(m) = g.as_ref() else { return };
        dump_map(m, "exit");
    }

    fn dump_map(m: &HashMap<usize, Rec>, when: &str) {
        // Group by backtrace.
        let mut groups: HashMap<(&'static str, &[usize]), (usize, usize)> = HashMap::new();
        for r in m.values() {
            let e = groups.entry((r.tname, &r.bt[..r.n])).or_insert((0, 0));
            e.0 += 1;
            e.1 += r.size;
        }
        let mut rows: Vec<(&'static str, &[usize], usize, usize)> =
            groups.into_iter().map(|((t, k), (c, b))| (t, k, c, b)).collect();
        rows.sort_by(|a, b| b.3.cmp(&a.3));
        let mut per_thread: HashMap<&'static str, usize> = HashMap::new();
        for r in m.values() {
            *per_thread.entry(r.tname).or_insert(0) += r.size;
        }
        let mut pt: Vec<_> = per_thread.into_iter().collect();
        pt.sort_by(|a, b| b.1.cmp(&a.1));
        for (t, b) in pt.iter().take(8) {
            eprintln!("ALLOC-TRACK thread: {t} live_bytes={b}");
        }
        let slide = unsafe { slide0() };
        eprintln!(
            "ALLOC-TRACK dump ({when}): {} live tracked blocks, {} bytes, slide=0x{:x}",
            m.len(),
            m.values().map(|r| r.size).sum::<usize>(),
            slide,
        );
        for (tname, bt, count, bytes) in rows.iter().take(25) {
            let addrs: Vec<String> = bt.iter().map(|a| format!("0x{a:x}")).collect();
            eprintln!("ALLOC-TRACK leak: thread={} n={} bytes={} bt={}", tname, count, bytes, addrs.join(" "));
        }
    }

    // ASLR slide for offline symbolication (atos) — a macOS dyld concept.
    // _dyld_get_image_vmaddr_slide does not exist on Linux (undefined
    // reference at link time on the CI cluster — t29 fold find); the Linux arm
    // reports 0 and symbolication uses /proc/<pid>/maps offsets instead.
    #[cfg(target_os = "macos")]
    unsafe extern "C" {
        fn _dyld_get_image_vmaddr_slide(image_index: u32) -> isize;
    }

    #[cfg(target_os = "macos")]
    unsafe fn slide0() -> isize {
        unsafe { _dyld_get_image_vmaddr_slide(0) }
    }

    #[cfg(not(target_os = "macos"))]
    unsafe fn slide0() -> isize {
        0
    }

    pub struct Tracker(pub mimalloc::MiMalloc);

    unsafe impl GlobalAlloc for Tracker {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { self.0.alloc(l) };
            record_alloc(p, l);
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { self.0.alloc_zeroed(l) };
            record_alloc(p, l);
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            record_free(p);
            unsafe { self.0.dealloc(p, l) };
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
            record_free(p);
            let np = unsafe { self.0.realloc(p, l, new_size) };
            let nl = Layout::from_size_align(new_size, l.align()).unwrap_or(l);
            record_alloc(np, nl);
            np
        }
    }
}

// Antithesis harness builds only: keep the coverage runtime linked. The
// crate does nothing without the sancov rustflags the harness image build
// supplies (root Dockerfile, ARG ANTITHESIS=1) — crate and flags are one
// unit. Target-scoped to the platform Antithesis runs on.
#[cfg(all(feature = "antithesis", target_os = "linux", target_arch = "x86_64"))]
use antithesis_instrumentation as _;

fn main() {
    // Antithesis harness builds: initialize the SDK before anything else so
    // assertion cataloging and lifecycle output are wired, then emit the
    // bootstrap reachability property from the one path every run executes.
    #[cfg(feature = "antithesis")]
    {
        antithesis_sdk::antithesis_init();
        antithesis_sdk::assert_reachable!("pgrust: server process entered main");
        // ParkLot steering probe: the parked thread reads its own
        // thread-local interrupt-pending flag, giving per-backend semantics
        // without a pgsync→backend dependency.
        pgsync::set_park_interrupt_probe(init_small::globals::InterruptPending);
    }
    // ipc::proc_exit ends the process by unwinding a ProcExitThread payload
    // rather than calling exit(2) — a thread must never _exit the shared
    // process. Nothing used to catch it here, so Rust's default panic path
    // turned even a CLEAN shutdown into exit code 101: `docker stop` and any
    // k8s liveness/restart policy read that as a crash. Carry the intended
    // code through instead. Real panics are re-raised untouched.
    // unwind-ok: log-then-die
    match std::panic::catch_unwind(run) {
        Ok(()) => {}
        Err(payload) => {
            if let Some(p) = payload.downcast_ref::<::ipc::ProcExitThread>() {
                std::process::exit(p.code);
            }
            std::panic::resume_unwind(payload);
        }
    }
}

fn run() {
    // Transport provider resolution (§2.4 seam): one argv peek, once, before
    // any seam install — set-once at boot is the house pattern. Everything
    // but `--stdio-wire` / `--stdio-wire-threaded` / `--sim-net` (all the C
    // dispatch options included) boots the socket provider, i.e. the
    // unchanged native byte path.
    let arg1 = std::env::args().nth(1);
    let transport = match arg1.as_deref() {
        Some("--stdio-wire") => seams_init::Transport::StdioWire,
        // Same provider, different thread: the threaded mode differs only in
        // WHICH thread runs the ladder (postgres/stdio_wire.rs).
        Some("--stdio-wire-threaded") => seams_init::Transport::StdioWire,
        #[cfg(pgrust_sim)]
        Some("--sim-net") => seams_init::Transport::SimNet,
        _ => seams_init::Transport::Socket,
    };
    seams_init::init_all_with_transport(transport);
    // mi_collect(force) releases mimalloc's freed-but-retained segments;
    // called only at alloc-churn boundaries (hashagg spill batch resets)
    // where retention would otherwise hold batch-sized RSS for the whole
    // spill pass.
    #[cfg(not(target_family = "wasm"))]
    mcx::set_allocator_release(|| unsafe { libmimalloc_sys::mi_collect(true) });
    // GL-MEMWATCH-1: allocator statistics for the memory watchdog's ledger —
    // mi_process_info is a cheap stats read (no heap walk). current_commit is
    // the r/w memory mimalloc has committed; the watchdog logs it beside RSS
    // and the accounted context bytes so allocator retention is separable
    // from truly untracked growth.
    #[cfg(not(target_family = "wasm"))]
    memwatchdog::set_allocator_stats(|| {
        let (mut rss, mut commit) = (0usize, 0usize);
        // SAFETY: nullable out-params per the mi_process_info contract.
        unsafe {
            libmimalloc_sys::mi_process_info(
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut rss,
                std::ptr::null_mut(),
                &mut commit,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }
        memwatchdog::AllocatorStats { current_rss: rss, current_commit: commit }
    });
    // GL-CONCMEM-1: the POOL-QOS memory governor's fallback usage basis for
    // hosts without /proc (the primary basis is RssAnon) — the allocator's
    // process RSS, the same cheap mi_process_info read as above.
    #[cfg(not(target_family = "wasm"))]
    runtime::install_qos_mem_probe(|| {
        let mut rss = 0usize;
        // SAFETY: nullable out-params per the mi_process_info contract.
        unsafe {
            libmimalloc_sys::mi_process_info(
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut rss,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }
        rss
    });
    // FPBUDGET-1 debug instrument: process-global live-context census
    // (name -> count), dumped at each backend exit. Diagnostic only.
    if std::env::var_os("PGRUST_MCXT_CENSUS").is_some() {
        mcx::debug_census::enable();
        eprintln!("MCXT-CENSUS: enabled (on={})", mcx::debug_census::on());
    }
    // FPBUDGET-1 debug instrument (debug builds): track backend-thread
    // allocations that survive to process exit, with fp backtraces.
    #[cfg(all(not(target_family = "wasm"), debug_assertions))]
    // PGRUST_ALLOC_TRACK=1 | all [,dump]: `all` records every thread, `dump`
    // installs the SIGWINCH mid-run dump (one env read: the determinism
    // lint's raw-env budget for this file is a ratchet).
    if let Some(v) = std::env::var_os("PGRUST_ALLOC_TRACK") {
        let v = v.to_string_lossy();
        let flag = |f: &str| v.split(',').any(|x| x.trim() == f);
        if flag("all") {
            alloc_track::ALL_THREADS.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        if flag("dump") {
            unsafe {
                libc::signal(libc::SIGWINCH, alloc_track::request_dump as extern "C" fn(libc::c_int) as usize);
            }
        }
        unsafe { libc::atexit(alloc_track::dump) };
        alloc_track::ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
        eprintln!("ALLOC-TRACK: enabled");
    }
    let argv: Vec<String> = main_main::argv_from_os(std::env::args_os());
    if let Err(e) = main_main::pg_main(&argv) {
        elog::emit_unhandled_error_report(&e);
        std::process::exit(1);
    }
}
