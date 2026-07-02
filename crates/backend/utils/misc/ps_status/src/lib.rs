//! Thread-model divergence from ps_status.c: one process hosts every backend
//! thread, so no argv/setproctitle trick can carry a per-backend title — the
//! per-thread buffer is authoritative and flush_ps_display is a no-op.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

// ps_status.c non-CLOBBER_ARGV branch.
pub const PS_BUFFER_SIZE: usize = 256;

const DEFAULT_UPDATE_PROCESS_TITLE: bool = true;

struct PsState {
    buffer: [u8; PS_BUFFER_SIZE],
    cur_len: usize,
    fixed_size: usize,
    nosuffix_len: usize,
    update_process_title: bool,
}

const _: () = assert!(!std::mem::needs_drop::<PsState>());

thread_local! {
    static STATE: RefCell<PsState> = const {
        RefCell::new(PsState {
            buffer: [0; PS_BUFFER_SIZE],
            cur_len: 0,
            fixed_size: 0,
            nosuffix_len: 0,
            update_process_title: DEFAULT_UPDATE_PROCESS_TITLE,
        })
    };
}

// C's `save_argv != NULL` gate; process-wide (main calls it once, pre-spawn).
static SAVED_ARGS: AtomicBool = AtomicBool::new(false);

pub fn update_process_title() -> bool {
    STATE.with(|s| s.borrow().update_process_title)
}

pub fn set_update_process_title(value: bool) {
    STATE.with(|s| s.borrow_mut().update_process_title = value);
}

pub fn save_ps_display_args() {
    SAVED_ARGS.store(true, Ordering::Relaxed);
}

fn update_ps_display_precheck(s: &PsState) -> bool {
    if !s.update_process_title {
        return false;
    }
    if !init_small::globals::IsUnderPostmaster() {
        return false;
    }
    if !SAVED_ARGS.load(Ordering::Relaxed) {
        return false;
    }
    true
}

// Bounded write at `at`, one byte reserved for C's NUL; returns the new end.
fn copy_capped(buffer: &mut [u8; PS_BUFFER_SIZE], at: usize, src: &[u8]) -> usize {
    let cap = PS_BUFFER_SIZE - 1;
    if at >= cap {
        return at;
    }
    let n = src.len().min(cap - at);
    buffer[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

pub fn init_ps_display(fixed_part: Option<&str>) {
    debug_assert!(
        fixed_part.is_some() || miscinit::GetMyBackendType() != types_core::BackendType::Invalid
    );
    let fixed_part =
        fixed_part.unwrap_or_else(|| miscinit::GetBackendTypeDesc(miscinit::GetMyBackendType()));

    if !init_small::globals::IsUnderPostmaster() {
        return;
    }
    if !SAVED_ARGS.load(Ordering::Relaxed) {
        return;
    }

    let cluster_name = guc_tables::vars::cluster_name.read();
    STATE.with(|s| {
        let s = &mut *s.borrow_mut();
        let mut at = copy_capped(&mut s.buffer, 0, b"postgres: ");
        match cluster_name.as_deref() {
            Some(name) if !name.is_empty() => {
                at = copy_capped(&mut s.buffer, at, name.as_bytes());
                at = copy_capped(&mut s.buffer, at, b": ");
            }
            _ => {}
        }
        at = copy_capped(&mut s.buffer, at, fixed_part.as_bytes());
        at = copy_capped(&mut s.buffer, at, b" ");
        s.cur_len = at;
        s.fixed_size = at;
        s.nosuffix_len = 0;
    });

    let save = update_process_title();
    set_update_process_title(true);
    set_ps_display("");
    set_update_process_title(save);
}

pub fn set_ps_display_suffix(suffix: &str) {
    STATE.with(|s| {
        let s = &mut *s.borrow_mut();
        if !update_ps_display_precheck(s) {
            return;
        }

        if s.nosuffix_len > 0 {
            s.cur_len = s.nosuffix_len;
        } else {
            s.nosuffix_len = s.cur_len;
        }

        let len = suffix.len();
        if s.cur_len + len + 1 >= PS_BUFFER_SIZE {
            if s.cur_len < PS_BUFFER_SIZE - 1 {
                s.buffer[s.cur_len] = b' ';
                s.cur_len += 1;
                s.cur_len = copy_capped(&mut s.buffer, s.cur_len, suffix.as_bytes());
            }
        } else {
            s.buffer[s.cur_len] = b' ';
            s.cur_len += 1;
            s.buffer[s.cur_len..s.cur_len + len].copy_from_slice(suffix.as_bytes());
            s.cur_len += len;
        }

        flush_ps_display(s);
    });
}

pub fn set_ps_display_remove_suffix() {
    STATE.with(|s| {
        let s = &mut *s.borrow_mut();
        if !update_ps_display_precheck(s) {
            return;
        }
        if s.nosuffix_len == 0 {
            return;
        }
        s.cur_len = s.nosuffix_len;
        s.nosuffix_len = 0;
        flush_ps_display(s);
    });
}

pub fn set_ps_display(activity: &str) {
    set_ps_display_with_len(activity, activity.len());
}

pub fn set_ps_display_with_len(activity: &str, len: usize) {
    debug_assert_eq!(activity.len(), len);

    STATE.with(|s| {
        let s = &mut *s.borrow_mut();
        if !update_ps_display_precheck(s) {
            return;
        }

        s.nosuffix_len = 0;

        if s.fixed_size + len >= PS_BUFFER_SIZE {
            s.cur_len = copy_capped(&mut s.buffer, s.fixed_size, activity.as_bytes());
        } else {
            s.buffer[s.fixed_size..s.fixed_size + len].copy_from_slice(activity.as_bytes());
            s.cur_len = s.fixed_size + len;
        }

        flush_ps_display(s);
    });
}

// C's kernel transmission point; per-thread none exists (module comment).
fn flush_ps_display(_s: &PsState) {}

// C returns (char *, displen) into ps_buffer; the TLS borrow reads via closure.
pub fn get_ps_display<R>(f: impl FnOnce(&[u8]) -> R) -> R {
    STATE.with(|s| {
        let s = s.borrow();
        f(&s.buffer[s.fixed_size..s.cur_len])
    })
}

pub fn init_seams() {
    ps_status_seams::init_ps_display::set(init_ps_display);
    ps_status_seams::set_ps_display::set(set_ps_display);
    ps_status_seams::set_ps_display_suffix::set(set_ps_display_suffix);
    ps_status_seams::set_ps_display_remove_suffix::set(set_ps_display_remove_suffix);
    guc_tables::vars::update_process_title.install(guc_tables::GucVarAccessors {
        get: update_process_title,
        set: set_update_process_title,
    });
}

#[cfg(test)]
mod tests;
