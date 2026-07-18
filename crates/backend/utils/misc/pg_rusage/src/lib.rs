//! pg_rusage.c: wall + CPU snapshot and "CPU: user: ..." delta formatting for
//! VERBOSE DDL messages.

use core::fmt::Write;

#[derive(Clone, Copy, Default)]
pub struct PgRUsage {
    tv_sec: i64,
    tv_usec: i64,
    ru_utime_sec: i64,
    ru_utime_usec: i64,
    ru_stime_sec: i64,
    ru_stime_usec: i64,
}

// C's getrusage(RUSAGE_SELF) measures the backend process; one backend here is
// one thread of a shared process, so per-thread usage is the parity reading.
#[cfg(target_os = "linux")]
const RUSAGE_WHO: libc::c_int = libc::RUSAGE_THREAD;
#[cfg(not(any(target_os = "linux", target_family = "wasm")))]
const RUSAGE_WHO: libc::c_int = libc::RUSAGE_SELF;

#[cfg(not(target_family = "wasm"))]
pub fn pg_rusage_init() -> PgRUsage {
    let mut tv = libc::timeval { tv_sec: 0, tv_usec: 0 };
    let mut ru: libc::rusage = unsafe { core::mem::zeroed() };
    unsafe {
        libc::gettimeofday(&mut tv, core::ptr::null_mut());
        libc::getrusage(RUSAGE_WHO, &mut ru);
    }
    PgRUsage {
        tv_sec: tv.tv_sec as i64,
        tv_usec: tv.tv_usec as i64,
        ru_utime_sec: ru.ru_utime.tv_sec as i64,
        ru_utime_usec: ru.ru_utime.tv_usec as i64,
        ru_stime_sec: ru.ru_stime.tv_sec as i64,
        ru_stime_usec: ru.ru_stime.tv_usec as i64,
    }
}

// wasm32: WASI p1 has no getrusage; VERBOSE rusage deltas read zero (the
// boot increment may route the wall leg through the clock seam instead).
#[cfg(target_family = "wasm")]
pub fn pg_rusage_init() -> PgRUsage {
    PgRUsage::default()
}

pub struct RUsageShow {
    buf: [u8; 100],
    len: usize,
}

impl RUsageShow {
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl Write for RUsageShow {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let take = s.len().min(self.buf.len() - self.len);
        self.buf[self.len..self.len + take].copy_from_slice(&s.as_bytes()[..take]);
        self.len += take;
        Ok(())
    }
}

pub fn pg_rusage_show(ru0: &PgRUsage) -> RUsageShow {
    let mut ru1 = pg_rusage_init();

    if ru1.tv_usec < ru0.tv_usec {
        ru1.tv_sec -= 1;
        ru1.tv_usec += 1_000_000;
    }
    if ru1.ru_stime_usec < ru0.ru_stime_usec {
        ru1.ru_stime_sec -= 1;
        ru1.ru_stime_usec += 1_000_000;
    }
    if ru1.ru_utime_usec < ru0.ru_utime_usec {
        ru1.ru_utime_sec -= 1;
        ru1.ru_utime_usec += 1_000_000;
    }

    let mut out = RUsageShow { buf: [0; 100], len: 0 };
    let _ = write!(
        out,
        "CPU: user: {}.{:02} s, system: {}.{:02} s, elapsed: {}.{:02} s",
        (ru1.ru_utime_sec - ru0.ru_utime_sec) as i32,
        ((ru1.ru_utime_usec - ru0.ru_utime_usec) / 10_000) as i32,
        (ru1.ru_stime_sec - ru0.ru_stime_sec) as i32,
        ((ru1.ru_stime_usec - ru0.ru_stime_usec) / 10_000) as i32,
        (ru1.tv_sec - ru0.tv_sec) as i32,
        ((ru1.tv_usec - ru0.tv_usec) / 10_000) as i32,
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_formats_like_c() {
        let ru0 = pg_rusage_init();
        let s = pg_rusage_show(&ru0);
        let s = s.as_str();
        assert!(s.starts_with("CPU: user: "), "{s}");
        assert!(s.contains(" s, system: ") && s.ends_with(" s"), "{s}");
    }

    #[test]
    fn usec_borrow_matches_c() {
        let mut ru0 = pg_rusage_init();
        // Force the borrow arms: ru1 usec below ru0 usec in all three clocks.
        ru0.tv_usec = 999_999;
        ru0.ru_utime_usec = 999_999;
        ru0.ru_stime_usec = 999_999;
        let s = pg_rusage_show(&ru0);
        assert!(s.as_str().starts_with("CPU: user: "), "{}", s.as_str());
    }
}
