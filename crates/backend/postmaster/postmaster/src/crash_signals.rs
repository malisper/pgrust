// C divergence: backends are threads, so a fatal signal kills the whole
// server with nothing in the log (C's postmaster survives the child and logs
// "terminated by signal N"). This handler emulates that log line, then
// restores the pre-existing disposition (Rust's stack-overflow reporter or
// SIG_DFL) and re-raises, so core/exit behavior is unchanged.

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

pub fn install_crash_signal_reporter() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    for (idx, (sig, _)) in CRASH_SIGS.iter().enumerate() {
        // SAFETY: standard sigaction install at single-threaded boot; the
        // handler is async-signal-safe (write + sigaction + raise only).
        unsafe {
            let mut sa: libc::sigaction = core::mem::zeroed();
            sa.sa_sigaction = crash_handler as usize;
            libc::sigfillset(&mut sa.sa_mask);
            sa.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
            libc::sigaction(*sig, &sa, (&raw mut PREV_ACTIONS[idx]).cast());
        }
    }
}

extern "C" fn crash_handler(sig: i32, info: *mut libc::siginfo_t, _uctx: *mut libc::c_void) {
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

    let mut buf = [0u8; 160];
    let mut w = Writer { buf: &mut buf, at: 0 };
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
    // SAFETY: write(2) is async-signal-safe; fd 2 is the server log.
    unsafe {
        libc::write(2, w.buf.as_ptr() as *const libc::c_void, w.at);
    }
    // Best-effort frame dump (glibc backtrace is not async-signal-safe; the
    // disposition is already restored, so a fault here dies under SIG_DFL).
    #[cfg(target_os = "linux")]
    // SAFETY: see above; addresses are written raw to fd 2.
    unsafe {
        let mut frames = [core::ptr::null_mut::<libc::c_void>(); 64];
        let n = libc::backtrace(frames.as_mut_ptr(), frames.len() as i32);
        if n > 0 {
            libc::backtrace_symbols_fd(frames.as_ptr(), n, 2);
        }
    }
    // SAFETY: raise(2) is async-signal-safe.
    unsafe {
        libc::raise(sig);
    }
}

struct Writer<'a> {
    buf: &'a mut [u8],
    at: usize,
}

impl Writer<'_> {
    fn put(&mut self, s: &[u8]) {
        let n = s.len().min(self.buf.len() - self.at);
        self.buf[self.at..self.at + n].copy_from_slice(&s[..n]);
        self.at += n;
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
