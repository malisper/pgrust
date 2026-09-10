// C divergence: backends are threads, so a fatal signal kills the whole
// server with nothing in the log (C's postmaster survives the child and logs
// "terminated by signal N"). This handler emulates that log line, then
// restores the pre-existing disposition (Rust's stack-overflow reporter or
// SIG_DFL) and re-raises SYNCHRONOUSLY; if the restored disposition
// handles-and-returns (only possible for a signal with no faulting
// instruction, e.g. kill-sent), it falls back to SIG_DFL so the process
// always dies — C3-F3, docs/design/crash-restart-gap.md §3.
//
// Every call made inside crash_handler is async-signal-safe (POSIX.1-2017
// §2.4.3) or touches only handler-local / kernel-supplied memory:
//   sigaction(2)        restore the saved disposition; force SIG_DFL
//   write(2)            Writer::flush, fd 2 = the server log
//   sigemptyset/sigaddset  sigset_t bit operations, no syscall
//   pthread_sigmask(3)  unblock the signal for the synchronous re-raise
//   raise(3)            the re-raise
//   walk_frames         guarded loads along the frame-pointer chain, no libc
//   CrashRegs::from_ucontext  plain loads from the kernel-supplied ucontext
//   current_fp          one inline `mov`
// No allocation, no locks, no backtrace(3), no dladdr(3), no formatting
// machinery — the Writer is a fixed stack buffer. glibc's backtrace(3)
// dlopen()s libgcc_s on first use (loader lock) and backtrace_symbols_fd(3)
// calls dladdr(3)/malloc(3): a fault inside the allocator or the dynamic
// loader then deadlocked the handler before the re-raise, so the server hung
// instead of dying and no core was written. Hence the raw frame dump.

use core::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

const CRASH_SIGS: [(i32, &str); 4] = [
    (libc::SIGSEGV, "SIGSEGV"),
    (libc::SIGBUS, "SIGBUS"),
    (libc::SIGILL, "SIGILL"),
    (libc::SIGABRT, "SIGABRT"),
];

static mut PREV_ACTIONS: [MaybeUninit<libc::sigaction>; 4] =
    [const { MaybeUninit::uninit() }; 4];
static INSTALLED: AtomicBool = AtomicBool::new(false);

#[allow(function_casts_as_integer)] // libc sa_sigaction is usize; handler-address cast is intentional
pub fn install_crash_signal_reporter() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    for (idx, (sig, _)) in CRASH_SIGS.iter().enumerate() {
        // SAFETY: standard sigaction install at single-threaded boot; the
        // handler is async-signal-safe (see the module comment).
        unsafe {
            let mut sa: libc::sigaction = core::mem::zeroed();
            sa.sa_sigaction = crash_handler as usize;
            libc::sigfillset(&mut sa.sa_mask);
            sa.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
            libc::sigaction(*sig, &sa, (&raw mut PREV_ACTIONS[idx]).cast());
        }
    }
}

extern "C" fn crash_handler(sig: i32, info: *mut libc::siginfo_t, uctx: *mut libc::c_void) {
    let mut name = "?";
    // Restore the previous disposition first: a fault inside this handler
    // then dies under it instead of recursing.
    for (idx, (s, n)) in CRASH_SIGS.iter().enumerate() {
        if *s == sig {
            name = n;
            // SAFETY: restoring the sigaction saved at install.
            unsafe {
                libc::sigaction(sig, (&raw const PREV_ACTIONS[idx]).cast(), core::ptr::null_mut());
            }
        }
    }

    let mut buf = [0u8; 256];
    let mut w = Writer { buf: &mut buf, at: 0, fd: 2 };
    w.put(b"\npgrust: FATAL: server process was terminated by signal ");
    w.dec(sig as u64);
    w.put(b" (");
    w.put(name.as_bytes());
    w.put(b"), fault address 0x");
    // SAFETY: siginfo pointer is kernel-provided and valid for SA_SIGINFO.
    #[cfg(target_os = "linux")]
    let addr = if info.is_null() { 0 } else { (unsafe { (*info).si_addr() }) as usize };
    #[cfg(not(target_os = "linux"))]
    let addr = if info.is_null() { 0 } else { (unsafe { (*info).si_addr }) as usize };
    w.hex(addr as u64);
    w.put(b"\n");
    w.flush();

    // Raw frame dump — async-signal-safe on Linux and macOS (module comment).
    // The disposition is already restored, so a bad load here dies under it.
    //
    // Symbolizing offline. Addresses are runtime (ASLR-slid); the "crash
    // handler at" line pins the slide the way the alloc-track dump's anchor
    // does: slide = <runtime crash handler> - <link-time crash_handler>.
    //   Linux:  L=$(nm -C target/<profile>/postgres | awk '/crash_handler$/{print $1}')
    //           addr2line -e target/<profile>/postgres -f -C -i \
    //               $(printf '0x%x' $(( frame - (handler_at - 0x$L) )))
    //   macOS:  L=$(nm target/<profile>/postgres | awk '/crash_handler$/{print $1}')
    //           atos -o target/<profile>/postgres -s $(printf '0x%x' $(( handler_at - 0x$L ))) <frame...>
    // (scripts/updfrom-crash-triage-e2e.sh and scripts/symbolize-segv-e2e.sh
    // are the addr2line precedents.) Frame 0 is the faulting pc, frame 1 the
    // link register (aarch64), the rest are return addresses along the
    // frame-pointer chain — the instruction after each call.
    let mut frames = [0usize; MAX_FRAMES];
    let n;
    w.put(b"pgrust: crash handler at 0x");
    w.hex(crash_handler as *const () as usize as u64);
    w.put(b", ");
    // SAFETY: uctx is kernel-provided and valid for SA_SIGINFO; layouts per
    // the platform's <sys/ucontext.h> (see CrashRegs).
    match unsafe { CrashRegs::from_ucontext(uctx) } {
        Some(r) => {
            w.put(b"pc 0x");
            w.hex(r.pc as u64);
            w.put(b", lr 0x");
            w.hex(r.lr as u64);
            w.put(b", fp 0x");
            w.hex(r.fp as u64);
            w.put(b"\n");
            frames[0] = r.pc;
            let mut k = 1;
            if r.lr != 0 {
                frames[1] = r.lr;
                k = 2;
            }
            // SAFETY: fp walk with monotonic + alignment + span guards.
            n = k + unsafe { walk_frames(r.fp, &mut frames[k..]) };
        }
        None => {
            w.put(b"no register context\n");
            // SAFETY: as above, from the handler's own frame (the chain
            // crosses the signal trampoline only where the platform links
            // it; a short dump beats none).
            n = unsafe { walk_frames(current_fp(), &mut frames) };
        }
    }
    for (i, f) in frames[..n].iter().enumerate() {
        w.put(b"pgrust: frame ");
        w.dec(i as u64);
        w.put(b": 0x");
        w.hex(*f as u64);
        w.put(b"\n");
    }
    w.put(b"pgrust: end of frames (");
    w.dec(n as u64);
    w.put(b")\n");
    w.flush();

    // C3-F3: deliver the re-raise NOW (the signal is blocked while this
    // handler runs, so unblock it first; raise(2) + sigprocmask(2) are
    // async-signal-safe). A kill(2)-delivered crash signal has no faulting
    // instruction to re-execute, and a restored disposition that RETURNS —
    // Rust's stack-overflow reporter returns for any non-guard-page SIGSEGV /
    // SIGBUS — would swallow a deferred re-raise, leaving a server that
    // logged FATAL yet kept serving (si_code cannot classify this: Darwin
    // stamps kill-sent SEGV/BUS with hardware-fault codes). Synchronous
    // delivery makes the swallow observable: if raise() returns, force
    // SIG_DFL and re-raise so the process dies as a C backend dies on
    // delivery. A genuine stack overflow still gets Rust's reporter message
    // on the first raise (it aborts rather than returning).
    // SAFETY: all calls are async-signal-safe (sigprocmask/raise/sigaction).
    unsafe {
        let mut only: libc::sigset_t = core::mem::zeroed();
        libc::sigemptyset(&mut only);
        libc::sigaddset(&mut only, sig);
        libc::pthread_sigmask(libc::SIG_UNBLOCK, &only, core::ptr::null_mut());
        libc::raise(sig);
        // Still here: the restored disposition handled-and-returned.
        let mut dfl: libc::sigaction = core::mem::zeroed();
        dfl.sa_sigaction = libc::SIG_DFL;
        libc::sigfillset(&mut dfl.sa_mask);
        libc::sigaction(sig, &dfl, core::ptr::null_mut());
        libc::raise(sig);
    }
}

/// Frame cap for the crash dump; `walk_frames` never returns more than this
/// even for a longer output slice.
pub const MAX_FRAMES: usize = 64;

/// Frame-pointer chain walk. Starting at the frame record `fp`, appends each
/// record's saved return address to `out` and returns the count. Guards, in
/// order: the cap, strictly increasing record addresses (stacks grow down,
/// so the caller's record sits above ours; a cycle or a garbage pointer
/// fails this), 8-byte alignment, a bounded hop between records (64 MiB,
/// wider than any thread stack here) and a return address above the zero
/// page. Async-signal-safe: two loads per frame and nothing else. Frame
/// layout is the AAPCS64 / SysV x86_64 one — [fp] = previous fp, [fp + 8]
/// = return address — which Rust's aarch64 and macOS targets keep for every
/// non-leaf function; a build without frame pointers walks few or no
/// frames rather than misreading. Same walk as alloc_track's `backtrace_fp`
/// in main_main's postgres.rs, parameterised on the start record.
///
/// # Safety
/// `fp` must be 0 (yields 0 frames) or point at a readable frame record whose
/// chain ends in a null / non-increasing / out-of-span pointer before it
/// would dereference unmapped memory.
pub unsafe fn walk_frames(fp: usize, out: &mut [usize]) -> usize {
    let cap = out.len().min(MAX_FRAMES);
    let mut fp = fp;
    let mut n = 0usize;
    let mut prev = 0usize;
    while n < cap && fp > prev && fp % 8 == 0 && (prev == 0 || fp - prev < (64 << 20)) {
        // SAFETY: guarded by the loop condition; see the doc comment.
        let ret = unsafe { core::ptr::read_volatile((fp + 8) as *const usize) };
        if ret < 0x1_0000 {
            break;
        }
        out[n] = ret;
        n += 1;
        prev = fp;
        // SAFETY: as above.
        fp = unsafe { core::ptr::read_volatile(fp as *const usize) };
    }
    n
}

/// The caller's frame pointer (0 where the target has no fp register we
/// know how to read).
#[inline(always)]
pub fn current_fp() -> usize {
    #[cfg(target_arch = "aarch64")]
    {
        let fp: usize;
        // SAFETY: reads x29; no memory access.
        unsafe {
            core::arch::asm!("mov {}, x29", out(reg) fp, options(nomem, nostack, preserves_flags));
        }
        fp
    }
    #[cfg(target_arch = "x86_64")]
    {
        let fp: usize;
        // SAFETY: reads rbp; no memory access.
        unsafe {
            core::arch::asm!("mov {}, rbp", out(reg) fp, options(nomem, nostack, preserves_flags));
        }
        fp
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        0
    }
}

/// The faulting thread's pc / lr / fp lifted out of the SA_SIGINFO ucontext,
/// so the walk starts at the faulting frame rather than at the handler on
/// its alternate stack (the trampoline between the two is not fp-linked on
/// Linux). Layouts are the platform's <sys/ucontext.h>; libc does not model
/// the Darwin ones, so they are spelled out by offset here.
struct CrashRegs {
    pc: usize,
    lr: usize,
    fp: usize,
}

impl CrashRegs {
    /// # Safety
    /// `uctx` is null or the kernel-supplied `ucontext_t*` of a SA_SIGINFO
    /// handler on this platform.
    #[allow(unused_variables, unreachable_code)]
    unsafe fn from_ucontext(uctx: *mut libc::c_void) -> Option<CrashRegs> {
        if uctx.is_null() {
            return None;
        }
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        {
            // SAFETY: per the contract; libc models this layout.
            let mc = unsafe { &(*(uctx as *const libc::ucontext_t)).uc_mcontext };
            return Some(CrashRegs {
                pc: mc.pc as usize,
                lr: mc.regs[30] as usize,
                fp: mc.regs[29] as usize,
            });
        }
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            // SAFETY: per the contract; libc models this layout.
            let mc = unsafe { &(*(uctx as *const libc::ucontext_t)).uc_mcontext };
            return Some(CrashRegs {
                pc: mc.gregs[libc::REG_RIP as usize] as usize,
                lr: 0,
                fp: mc.gregs[libc::REG_RBP as usize] as usize,
            });
        }
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        {
            // <sys/_types/_ucontext.h> + <mach/arm/_structs.h>:
            //   ucontext_t { int uc_onstack; sigset_t uc_sigmask;      // 0, 4
            //                stack_t uc_stack;                         // 8
            //                ucontext_t *uc_link; size_t uc_mcsize;    // 32, 40
            //                mcontext_t uc_mcontext; }                 // 48
            //   __darwin_mcontext64 { __es { u64 far; u32 esr; u32 exc; }  // 16 bytes
            //                         __ss { u64 x[29]; u64 fp, lr, sp, pc;
            //                                u32 cpsr, pad; } __ns { .. } }
            #[repr(C)]
            struct DarwinSs {
                x: [u64; 29],
                fp: u64,
                lr: u64,
                sp: u64,
                pc: u64,
            }
            const UC_MCONTEXT_OFF: usize = 48;
            const SS_OFF: usize = 16;
            // SAFETY: per the contract and the layout above.
            unsafe {
                let mc = *((uctx as *const u8).add(UC_MCONTEXT_OFF) as *const *const u8);
                if mc.is_null() {
                    return None;
                }
                let ss = &*(mc.add(SS_OFF) as *const DarwinSs);
                return Some(CrashRegs { pc: ss.pc as usize, lr: ss.lr as usize, fp: ss.fp as usize });
            }
        }
        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        {
            // As above with <mach/i386/_structs.h>: __es is 16 bytes; __ss is
            // { rax rbx rcx rdx rdi rsi rbp rsp r8..r15 rip rflags cs fs gs }.
            const UC_MCONTEXT_OFF: usize = 48;
            const RBP_OFF: usize = 16 + 6 * 8;
            const RIP_OFF: usize = 16 + 16 * 8;
            // SAFETY: per the contract and the layout above.
            unsafe {
                let mc = *((uctx as *const u8).add(UC_MCONTEXT_OFF) as *const *const u8);
                if mc.is_null() {
                    return None;
                }
                let rbp = *(mc.add(RBP_OFF) as *const u64);
                let rip = *(mc.add(RIP_OFF) as *const u64);
                return Some(CrashRegs { pc: rip as usize, lr: 0, fp: rbp as usize });
            }
        }
        None
    }
}

struct Writer<'a> {
    buf: &'a mut [u8],
    at: usize,
    /// Destination fd: 2 (the server log) in the handler; a pipe in tests.
    fd: libc::c_int,
}

impl Writer<'_> {
    /// Append, flushing to the log whenever the buffer fills so a long dump
    /// is never truncated (no allocation; the buffer is the caller's).
    fn put(&mut self, mut s: &[u8]) {
        while !s.is_empty() {
            if self.at == self.buf.len() {
                self.flush();
            }
            let n = s.len().min(self.buf.len() - self.at);
            self.buf[self.at..self.at + n].copy_from_slice(&s[..n]);
            self.at += n;
            s = &s[n..];
        }
    }
    /// write(2) the buffered bytes to fd 2 (the server log) and reset. The
    /// only fs site in this file (lint-determinism.allow); async-signal-safe
    /// by POSIX, retried on a short write, abandoned on error.
    fn flush(&mut self) {
        let mut off = 0usize;
        while off < self.at {
            // SAFETY: write(2) on an open fd from a fixed stack buffer.
            let r = unsafe { libc::write(self.fd, self.buf[off..].as_ptr() as *const libc::c_void, self.at - off) };
            if r <= 0 {
                break;
            }
            off += r as usize;
        }
        self.at = 0;
    }
    fn dec(&mut self, mut v: u64) {
        let mut tmp = [0u8; 20];
        let mut i = tmp.len();
        loop {
            i -= 1;
            tmp[i] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        let s: [u8; 20] = tmp;
        self.put(&s[i..]);
    }
    fn hex(&mut self, mut v: u64) {
        let mut tmp = [0u8; 16];
        let mut i = tmp.len();
        loop {
            i -= 1;
            tmp[i] = b"0123456789abcdef"[(v & 0xf) as usize];
            v >>= 4;
            if v == 0 {
                break;
            }
        }
        let s: [u8; 16] = tmp;
        self.put(&s[i..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A synthetic AAPCS64/SysV frame record: [fp] = previous fp, [fp+8] = ret.
    #[repr(C, align(16))]
    #[derive(Clone, Copy)]
    struct Rec {
        next: usize,
        ret: usize,
    }

    fn chain(n: usize) -> Vec<Rec> {
        let mut v = vec![Rec { next: 0, ret: 0 }; n];
        for i in 0..n {
            v[i].ret = 0x10_0000 + i * 4;
        }
        for i in 0..n.saturating_sub(1) {
            v[i].next = &v[i + 1] as *const Rec as usize;
        }
        v
    }

    #[test]
    fn walk_frames_synthetic_chain_in_order() {
        let c = chain(5);
        let mut out = [0usize; MAX_FRAMES];
        let n = unsafe { walk_frames(&c[0] as *const Rec as usize, &mut out) };
        assert_eq!(n, 5);
        for i in 0..5 {
            assert_eq!(out[i], 0x10_0000 + i * 4, "frame {i}");
        }
    }

    #[test]
    fn walk_frames_stops_on_non_monotonic_link() {
        let mut c = chain(4);
        // A cycle back to the head must stop the walk after the 3rd record.
        c[2].next = &c[0] as *const Rec as usize;
        let mut out = [0usize; MAX_FRAMES];
        let n = unsafe { walk_frames(&c[0] as *const Rec as usize, &mut out) };
        assert_eq!(n, 3);
    }

    #[test]
    fn walk_frames_stops_on_zero_page_return_and_misalignment() {
        let mut c = chain(4);
        c[1].ret = 0x10; // zero-page return address: not a code address
        let mut out = [0usize; MAX_FRAMES];
        let n = unsafe { walk_frames(&c[0] as *const Rec as usize, &mut out) };
        assert_eq!(n, 1);
        let mut c = chain(3);
        c[0].next = &c[1] as *const Rec as usize + 4; // misaligned link
        let n = unsafe { walk_frames(&c[0] as *const Rec as usize, &mut out) };
        assert_eq!(n, 1);
        assert_eq!(unsafe { walk_frames(0, &mut out) }, 0);
    }

    #[test]
    fn walk_frames_bounded_to_cap() {
        let c = chain(MAX_FRAMES + 8);
        let mut out = [0usize; MAX_FRAMES + 8];
        let n = unsafe { walk_frames(&c[0] as *const Rec as usize, &mut out) };
        assert_eq!(n, MAX_FRAMES, "walk honours MAX_FRAMES");
        let mut short = [0usize; 3];
        let n = unsafe { walk_frames(&c[0] as *const Rec as usize, &mut short) };
        assert_eq!(n, 3, "walk honours a shorter output slice");
    }

    #[test]
    fn walk_frames_live_chain_is_bounded_and_code_addressed() {
        let mut out = [0usize; MAX_FRAMES];
        let n = unsafe { walk_frames(current_fp(), &mut out) };
        assert!(n <= MAX_FRAMES);
        // Frame pointers are kept on every target the server ships on
        // (aarch64 non-leaf frames; Apple x86_64 ABI), so a live walk from
        // a non-leaf test function sees at least its caller.
        #[cfg(any(target_arch = "aarch64", target_os = "macos"))]
        assert!(n >= 1, "live frame-pointer walk found no frames");
        for f in &out[..n] {
            assert!(*f >= 0x1_0000, "frame {f:#x} below the zero page");
        }
    }

    #[test]
    fn writer_flushes_when_full_without_truncating() {
        // 2 KiB through a 16-byte buffer into a pipe: the put path must
        // chunk-and-flush rather than drop the tail, and the bytes must
        // arrive in order and complete.
        let mut fds = [0 as libc::c_int; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let reader = std::thread::spawn(move || {
            let mut got = Vec::new();
            let mut chunk = [0u8; 512];
            loop {
                let r = unsafe { libc::read(fds[0], chunk.as_mut_ptr().cast(), chunk.len()) };
                if r <= 0 {
                    break;
                }
                got.extend_from_slice(&chunk[..r as usize]);
            }
            unsafe { libc::close(fds[0]) };
            got
        });
        let mut buf = [0u8; 16];
        let mut w = Writer { buf: &mut buf, at: 0, fd: fds[1] };
        let big = [b'x'; 2048];
        w.put(&big);
        w.put(b"|");
        w.hex(u64::MAX);
        w.put(b"|");
        w.dec(u64::MAX);
        w.put(b"|");
        w.hex(0);
        w.put(b"|");
        w.dec(0);
        w.put(b"\n");
        assert!(w.at <= 16);
        w.flush();
        assert_eq!(w.at, 0);
        unsafe { libc::close(fds[1]) };
        let got = reader.join().unwrap();
        let expect = format!("{}|ffffffffffffffff|18446744073709551615|0|0\n", "x".repeat(2048));
        assert_eq!(String::from_utf8(got).unwrap(), expect);
    }
}
