//! Port of `src/backend/utils/misc/ps_status.c`.
//!
//! The C file keeps the process-title buffer and the `update_process_title`
//! GUC in `static` globals; those are per-backend state, so they are
//! `thread_local!` here. The buffer-management logic (fixed prefix, activity,
//! suffix, and the bounded truncation that mirrors C's fixed-size `ps_buffer`)
//! is reproduced exactly; `flush_ps_display` transmits the rendered title to
//! the kernel.
//!
//! Platform note: PostgreSQL selects `PS_USE_CLOBBER_ARGV` on
//! Linux/macOS/Solaris, overwriting the original `argv`/`environ` region. That
//! requires the raw `argv` pointers `main()` was handed, which this crate does
//! not have; `save_ps_display_args` therefore records the available buffer
//! size (the only datum the buffer logic needs) and `flush_ps_display` writes
//! the title via `setproctitle` where the platform provides it, otherwise the
//! tracked buffer is authoritative (the `get_ps_display` accessor and all
//! callers see the correct value). The argv-clobber transmission itself is the
//! single platform machinery not reproduced here.

use std::cell::RefCell;

use init_small::globals::IsUnderPostmaster;
use guc_tables::vars;
use miscinit_seams::get_backend_type_desc;
use init_small_seams::my_backend_type;

/// `#define DEFAULT_UPDATE_PROCESS_TITLE true` (ps_status.h on the
/// CLOBBER_ARGV/setproctitle platforms; the GUC boot value is also `true`).
const DEFAULT_UPDATE_PROCESS_TITLE: bool = true;

struct PsState {
    /// `bool update_process_title` GUC variable storage (owned by ps_status.c).
    update_process_title: bool,
    /// Whether `save_ps_display_args` ran (`save_argv != NULL` in C).
    saved_args: bool,
    /// `ps_buffer_size` — the bound on the title buffer.
    buffer_size: usize,
    /// `ps_buffer` — the title contents (nominal `strlen` == `buffer.len()`,
    /// the C `ps_buffer_cur_len`).
    buffer: String,
    /// `ps_buffer_fixed_size` — length of the constant prefix.
    fixed_size: usize,
    /// `ps_buffer_nosuffix_len` — length before a suffix was appended, or
    /// `None`/0 when no suffix is set.
    nosuffix_len: Option<usize>,
}

impl PsState {
    const fn new() -> Self {
        PsState {
            update_process_title: DEFAULT_UPDATE_PROCESS_TITLE,
            saved_args: false,
            buffer_size: 0,
            buffer: String::new(),
            fixed_size: 0,
            nosuffix_len: None,
        }
    }
}

thread_local! {
    static STATE: RefCell<PsState> = const { RefCell::new(PsState::new()) };
}

/// `update_process_title` GUC value.
pub fn update_process_title() -> bool {
    STATE.with(|s| s.borrow().update_process_title)
}

/// Set `update_process_title`, returning the previous value (the C
/// save/restore idiom in `init_ps_display`).
pub fn set_update_process_title(value: bool) -> bool {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        let old = s.update_process_title;
        s.update_process_title = value;
        old
    })
}

/// `save_ps_display_args(argc, argv)` — record the original argv and the size
/// of the title buffer. The C counts the contiguous argv/environ region; this
/// port records the summed argument bytes plus a NUL each as the buffer bound
/// (the value the rest of the buffer logic consumes).
pub fn save_ps_display_args(args: &[impl AsRef<str>]) {
    let total: usize = args.iter().map(|a| a.as_ref().len() + 1).sum();
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        s.saved_args = true;
        s.buffer_size = total;
    });
}

/// `update_ps_display_precheck()` — whether updating the process title is
/// something we need to do.
fn update_ps_display_precheck(s: &PsState) -> bool {
    // update_process_title=off disables updates.
    if !s.update_process_title {
        return false;
    }
    // No ps display for stand-alone backend.
    if !IsUnderPostmaster() {
        return false;
    }
    // PS_USE_CLOBBER_ARGV: if ps_buffer is a pointer it might still be null —
    // here, if save_ps_display_args() was never called there is no buffer.
    if !s.saved_args {
        return false;
    }
    true
}

/// `init_ps_display(fixed_part)` — build the fixed title prefix
/// (`postgres: [cluster: ]fixed_part `). `None` mirrors C's `NULL`: derive the
/// fixed part from `MyBackendType`.
pub fn init_ps_display(fixed_part: Option<&str>) -> types_error::PgResult<()> {
    // Assert(fixed_part || MyBackendType); the description fallback.
    let owned_desc;
    let fixed_part = match fixed_part {
        Some(p) => p,
        None => {
            owned_desc = get_backend_type_desc::call(my_backend_type::call());
            owned_desc
        }
    };

    // No ps display for stand-alone backend.
    if !IsUnderPostmaster() {
        return Ok(());
    }

    STATE.with(|s| {
        let mut s = s.borrow_mut();

        // No ps display if you didn't call save_ps_display_args().
        if !s.saved_args {
            return;
        }

        // Make fixed prefix of ps display.
        //
        // On the setproctitle platforms the prefix has no "postgres: "
        // (setproctitle adds a `progname:` itself); on CLOBBER_ARGV/none it
        // does. macOS/Linux/Solaris use CLOBBER_ARGV, so the prefix is
        // "postgres: ".
        let cluster_name = (vars::cluster_name.get().get)();
        let buffer_size = s.buffer_size;
        s.buffer.clear();
        match cluster_name.as_deref() {
            Some(name) if !name.is_empty() => {
                push_truncated(&mut s.buffer, buffer_size, "postgres: ");
                push_truncated(&mut s.buffer, buffer_size, name);
                push_truncated(&mut s.buffer, buffer_size, ": ");
                push_truncated(&mut s.buffer, buffer_size, fixed_part);
                push_truncated(&mut s.buffer, buffer_size, " ");
            }
            _ => {
                push_truncated(&mut s.buffer, buffer_size, "postgres: ");
                push_truncated(&mut s.buffer, buffer_size, fixed_part);
                push_truncated(&mut s.buffer, buffer_size, " ");
            }
        }
        s.fixed_size = s.buffer.len();
        s.nosuffix_len = None;
    });

    // On the first run, force the update.
    let save = set_update_process_title(true);
    set_ps_display_with_len("", 0);
    set_update_process_title(save);
    Ok(())
}

/// `set_ps_display_suffix(suffix)` — append (or overwrite) a trailing suffix.
pub fn set_ps_display_suffix(suffix: &str) {
    let title = STATE.with(|s| {
        let mut s = s.borrow_mut();
        if !update_ps_display_precheck(&s) {
            return None;
        }

        // If there's already a suffix, overwrite it.
        let restore_len = match s.nosuffix_len {
            Some(len) if len > 0 => len,
            _ => {
                let len = s.buffer.len();
                s.nosuffix_len = Some(len);
                len
            }
        };
        s.buffer.truncate(restore_len);

        let buffer_size = s.buffer_size;
        // C checks `ps_buffer_cur_len + len + 1 >= ps_buffer_size`; only append
        // the space when there is room (buffer not already full).
        let len = suffix.len();
        if s.buffer.len() + len + 1 >= buffer_size {
            if s.buffer.len() < buffer_size.saturating_sub(1) {
                push_truncated(&mut s.buffer, buffer_size, " ");
                push_truncated(&mut s.buffer, buffer_size, suffix);
            }
        } else {
            push_truncated(&mut s.buffer, buffer_size, " ");
            push_truncated(&mut s.buffer, buffer_size, suffix);
        }
        Some(s.buffer.clone())
    });
    if let Some(title) = title {
        flush_ps_display(&title);
    }
}

/// `set_ps_display_remove_suffix()` — drop the trailing suffix.
pub fn set_ps_display_remove_suffix() {
    let title = STATE.with(|s| {
        let mut s = s.borrow_mut();
        if !update_ps_display_precheck(&s) {
            return None;
        }
        // Check we added a suffix.
        match s.nosuffix_len {
            Some(len) if len > 0 => {
                s.buffer.truncate(len);
                s.nosuffix_len = None;
                Some(s.buffer.clone())
            }
            _ => None,
        }
    });
    if let Some(title) = title {
        flush_ps_display(&title);
    }
}

/// `set_ps_display(activity)` — replace the activity part of the title.
pub fn set_ps_display(activity: &str) {
    set_ps_display_with_len(activity, activity.len());
}

/// `set_ps_display_with_len(activity, len)` — replace the activity part of the
/// title; `len` must equal `strlen(activity)`.
pub fn set_ps_display_with_len(activity: &str, len: usize) {
    debug_assert_eq!(activity.len(), len, "Assert(strlen(activity) == len)");

    let title = STATE.with(|s| {
        let mut s = s.borrow_mut();
        if !update_ps_display_precheck(&s) {
            return None;
        }

        // Wipe out any suffix when the title is completely changed.
        s.nosuffix_len = None;

        let buffer_size = s.buffer_size;
        let truncate_to = s.fixed_size.min(s.buffer.len());
        s.buffer.truncate(truncate_to);
        push_truncated(&mut s.buffer, buffer_size, activity);
        Some(s.buffer.clone())
    });
    if let Some(title) = title {
        flush_ps_display(&title);
    }
}

/// `get_ps_display(displen)` — the activity portion of the title (the buffer
/// with the fixed prefix removed) and its byte length.
pub fn get_ps_display() -> (String, usize) {
    STATE.with(|s| {
        let s = s.borrow();
        let start = s.fixed_size.min(s.buffer.len());
        let display = s.buffer[start..].to_owned();
        let len = display.len();
        (display, len)
    })
}

/// `flush_ps_display()` — transmit the new title to the kernel.
fn flush_ps_display(title: &str) {
    os_set_proc_title(title);
}

/// Append `value` to `buffer`, truncating at `buffer_size` on a char boundary.
/// The safe analogue of C's bounded `memcpy` into the fixed `ps_buffer`: it
/// never grows past `buffer_size - 1` (one byte reserved for the NUL).
fn push_truncated(buffer: &mut String, buffer_size: usize, value: &str) {
    if buffer_size == 0 {
        return;
    }
    let limit = buffer_size - 1;
    let available = limit.saturating_sub(buffer.len());
    if available == 0 {
        return;
    }
    if value.len() <= available {
        buffer.push_str(value);
        return;
    }
    let mut end = available;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    buffer.push_str(&value[..end]);
}

#[cfg(any(target_os = "freebsd", target_os = "openbsd", target_os = "netbsd"))]
fn os_set_proc_title(title: &str) {
    // PS_USE_SETPROCTITLE: setproctitle("%s", ps_buffer).
    if let Ok(c) = std::ffi::CString::new(title) {
        let fmt = c"%s";
        unsafe {
            libc::setproctitle(fmt.as_ptr(), c.as_ptr());
        }
    }
}

#[cfg(not(any(target_os = "freebsd", target_os = "openbsd", target_os = "netbsd")))]
fn os_set_proc_title(_title: &str) {
    // PS_USE_CLOBBER_ARGV (Linux/macOS/Solaris) overwrites the argv/environ
    // region, which needs the raw argv this crate does not hold; the tracked
    // buffer above is authoritative for callers. No-op transmission.
}
