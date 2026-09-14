//! src/port/path.c (Unix arms) + the exec-location half of src/common/exec.c
//! (validate_exec, find_my_exec, find_other_exec, pipe_read_line,
//! pclose_check, set_pglocale_pgservice).

use std::ffi::{CStr, CString};

#[cfg(test)]
mod tests;

pub const MAXPGPATH: usize = 1024;

// DIVERGENCE: C's compiled-in dirs come from the build-generated
// pg_config_paths.h; here the PGRUST_* build env stands in (pgtz precedent),
// defaulting to the documented --prefix=/usr/local/pgsql layout.
const PGBINDIR: &str = match option_env!("PGRUST_PGBINDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/bin",
};
const PGSHAREDIR: &str = match option_env!("PGRUST_PGSHAREDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/share",
};
const SYSCONFDIR: &str = match option_env!("PGRUST_SYSCONFDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/etc",
};
const INCLUDEDIR: &str = match option_env!("PGRUST_INCLUDEDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/include",
};
const PKGINCLUDEDIR: &str = match option_env!("PGRUST_PKGINCLUDEDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/include",
};
const INCLUDEDIRSERVER: &str = match option_env!("PGRUST_INCLUDEDIRSERVER") {
    Some(v) => v,
    None => "/usr/local/pgsql/include/server",
};
const LIBDIR: &str = match option_env!("PGRUST_LIBDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/lib",
};
const PKGLIBDIR: &str = match option_env!("PGRUST_PKGLIBDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/lib",
};
const LOCALEDIR: &str = match option_env!("PGRUST_LOCALEDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/share/locale",
};
const DOCDIR: &str = match option_env!("PGRUST_DOCDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/share/doc",
};
const HTMLDIR: &str = match option_env!("PGRUST_HTMLDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/share/doc",
};
const MANDIR: &str = match option_env!("PGRUST_MANDIR") {
    Some(v) => v,
    None => "/usr/local/pgsql/share/man",
};

#[inline]
fn is_dir_sep(ch: u8) -> bool {
    ch == b'/'
}

#[inline]
fn is_path_var_sep(ch: u8) -> bool {
    ch == b':'
}

#[inline]
pub fn is_absolute_path(path: &str) -> bool {
    path.as_bytes().first() == Some(&b'/')
}

pub fn first_dir_separator(filename: &str) -> Option<usize> {
    filename.bytes().position(is_dir_sep)
}

// Sound only on canonicalized paths: ".." can then appear only at the start.
pub fn path_contains_parent_reference(path: &str) -> bool {
    let b = path.as_bytes();
    b.starts_with(b"..") && (b.len() == 2 || is_dir_sep(b[2]))
}

pub fn path_is_relative_and_below_cwd(path: &str) -> bool {
    !is_absolute_path(path) && !path_contains_parent_reference(path)
}

pub fn path_is_prefix_of_path(path1: &str, path2: &str) -> bool {
    let (b1, b2) = (path1.as_bytes(), path2.as_bytes());
    b2.starts_with(b1) && (b2.len() == b1.len() || is_dir_sep(b2[b1.len()]))
}

/// strlcpy(buf, s, MAXPGPATH): the longest prefix of at most MAXPGPATH-1
/// bytes, backed off to a char boundary so the slice stays valid UTF-8.
fn truncate_maxpgpath(s: &str) -> &str {
    let mut n = s.len().min(MAXPGPATH - 1);
    while !s.is_char_boundary(n) {
        n -= 1;
    }
    &s[..n]
}

// path.c:637: is this (archive member / relative) path safe to extract
// below the current directory — after canonicalization, neither absolute
// nor reaching above cwd. Frontend-side check shared by astreamer_file,
// astreamer_tar and pg_rewind's file map.
pub fn path_is_safe_for_extraction(path: &str) -> bool {
    let buf = canonicalize_path(truncate_maxpgpath(path));
    path_is_relative_and_below_cwd(&buf)
}

pub fn first_path_var_separator(pathlist: &str) -> Option<usize> {
    pathlist.bytes().position(is_path_var_sep)
}

pub fn last_dir_separator(filename: &str) -> Option<usize> {
    filename.bytes().rposition(is_dir_sep)
}

pub fn join_path_components(head: &str, tail: &str) -> String {
    let mut ret = String::with_capacity(head.len() + tail.len() + 1);
    ret.push_str(head);
    if !tail.is_empty() {
        if !head.is_empty() {
            ret.push('/');
        }
        ret.push_str(tail);
    }
    ret
}

fn trim_trailing_separator(path: &mut Vec<u8>) {
    let mut end = path.len();
    while end > 1 && is_dir_sep(path[end - 1]) {
        end -= 1;
    }
    path.truncate(end);
}

fn trim_directory(path: &mut Vec<u8>) {
    if path.is_empty() {
        return;
    }
    let mut p = path.len() - 1;
    while is_dir_sep(path[p]) && p > 0 {
        p -= 1;
    }
    while !is_dir_sep(path[p]) && p > 0 {
        p -= 1;
    }
    while p > 0 && is_dir_sep(path[p - 1]) {
        p -= 1;
    }
    if p == 0 && !path.is_empty() && is_dir_sep(path[0]) {
        p = 1;
    }
    path.truncate(p);
}

pub fn get_parent_directory(path: &str) -> String {
    let mut buf = path.as_bytes().to_vec();
    trim_directory(&mut buf);
    String::from_utf8(buf).expect("trim_directory truncates on ASCII '/' boundaries")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CanonState {
    AbsoluteInit,
    AbsoluteWithDepth,
    RelativeInit,
    RelativeWithDepth,
    RelativeWithParentRef,
}

pub fn canonicalize_path(input: &str) -> String {
    String::from_utf8(canonicalize_path_bytes(input.as_bytes()))
        .expect("components sliced on ASCII '/' boundaries")
}

// canonicalize_path (path.c) over raw bytes: the C routine works on char*
// and only ever inspects ASCII '/', '.', so non-UTF-8 filenames (a SQL_ASCII
// database's text) pass through byte-for-byte.
pub fn canonicalize_path_bytes(input: &[u8]) -> Vec<u8> {
    let mut path: Vec<u8> = input.to_vec();
    trim_trailing_separator(&mut path);

    let mut dedup: Vec<u8> = Vec::with_capacity(path.len());
    let mut was_sep = false;
    for &b in &path {
        if b == b'/' && was_sep {
            continue;
        }
        dedup.push(b);
        was_sep = b == b'/';
    }
    let path = dedup;

    if path.is_empty() {
        return Vec::new();
    }

    let absolute = path[0] == b'/';
    let body: &[u8] = if absolute { &path[1..] } else { &path[..] };

    let mut dirs: Vec<&[u8]> = Vec::new();
    let mut state = if absolute {
        CanonState::AbsoluteInit
    } else {
        CanonState::RelativeInit
    };
    let mut pathdepth: i32 = 0;

    for comp in body.split(|&b| b == b'/') {
        if comp.is_empty() || comp == b"." {
            continue;
        }
        let is_double_dot = comp == b"..";
        match state {
            CanonState::AbsoluteInit => {
                if !is_double_dot {
                    dirs.push(comp);
                    state = CanonState::AbsoluteWithDepth;
                    pathdepth += 1;
                }
            }
            CanonState::AbsoluteWithDepth => {
                if is_double_dot {
                    dirs.pop();
                    pathdepth -= 1;
                    if pathdepth == 0 {
                        state = CanonState::AbsoluteInit;
                    }
                } else {
                    dirs.push(comp);
                    pathdepth += 1;
                }
            }
            CanonState::RelativeInit => {
                if is_double_dot {
                    dirs.push(comp);
                    state = CanonState::RelativeWithParentRef;
                } else {
                    dirs.push(comp);
                    state = CanonState::RelativeWithDepth;
                    pathdepth += 1;
                }
            }
            CanonState::RelativeWithDepth => {
                if is_double_dot {
                    dirs.pop();
                    pathdepth -= 1;
                    if pathdepth == 0 {
                        state = if dirs.is_empty() {
                            CanonState::RelativeInit
                        } else {
                            CanonState::RelativeWithParentRef
                        };
                    }
                } else {
                    dirs.push(comp);
                    pathdepth += 1;
                }
            }
            CanonState::RelativeWithParentRef => {
                if !is_double_dot {
                    state = CanonState::RelativeWithDepth;
                    pathdepth = 1;
                }
                dirs.push(comp);
            }
        }
    }

    let mut out: Vec<u8> = Vec::with_capacity(path.len());
    if absolute {
        out.push(b'/');
    }
    for (i, d) in dirs.iter().enumerate() {
        if i > 0 {
            out.push(b'/');
        }
        out.extend_from_slice(d);
    }
    if !absolute && out.is_empty() {
        out.push(b'.');
    }
    out
}

fn dir_strcmp(s1: &[u8], s2: &[u8]) -> i32 {
    let n = s1.len().min(s2.len());
    for i in 0..n {
        let (a, b) = (s1[i], s2[i]);
        if a != b && !(is_dir_sep(a) && is_dir_sep(b)) {
            return a as i32 - b as i32;
        }
    }
    (s1.len() > n) as i32 - (s2.len() > n) as i32
}

fn make_relative_path(target_path: &str, bin_path: &str, my_exec_path: &str) -> String {
    let target = target_path.as_bytes();
    let bin = bin_path.as_bytes();

    // Common prefix must end on a directory separator ('/usr/lib' vs
    // '/usr/libexec').
    let mut prefix_len = 0usize;
    let mut i = 0usize;
    while i < target.len() && i < bin.len() {
        if is_dir_sep(target[i]) && is_dir_sep(bin[i]) {
            prefix_len = i + 1;
        } else if target[i] != bin[i] {
            break;
        }
        i += 1;
    }
    if prefix_len == 0 {
        return canonicalize_path(target_path);
    }
    let tail_len = bin.len() - prefix_len;

    let mut ret: Vec<u8> = my_exec_path.as_bytes().to_vec();
    ret.truncate(MAXPGPATH - 1);
    trim_directory(&mut ret);
    let canon = canonicalize_path(&String::from_utf8_lossy(&ret));
    let ret = canon.as_bytes();

    let tail_start = ret.len() as isize - tail_len as isize;
    if tail_start > 0 {
        let ts = tail_start as usize;
        if is_dir_sep(ret[ts - 1]) && dir_strcmp(&ret[ts..], &bin[prefix_len..]) == 0 {
            let mut head = ret[..ts].to_vec();
            trim_trailing_separator(&mut head);
            let head = String::from_utf8_lossy(&head).into_owned();
            let joined = join_path_components(&head, &target_path[prefix_len..]);
            return canonicalize_path(&joined);
        }
    }
    canonicalize_path(target_path)
}

pub fn get_share_path(my_exec_path: &str) -> String {
    make_relative_path(PGSHAREDIR, PGBINDIR, my_exec_path)
}

pub fn get_etc_path(my_exec_path: &str) -> String {
    make_relative_path(SYSCONFDIR, PGBINDIR, my_exec_path)
}

pub fn get_include_path(my_exec_path: &str) -> String {
    make_relative_path(INCLUDEDIR, PGBINDIR, my_exec_path)
}

pub fn get_pkginclude_path(my_exec_path: &str) -> String {
    make_relative_path(PKGINCLUDEDIR, PGBINDIR, my_exec_path)
}

pub fn get_includeserver_path(my_exec_path: &str) -> String {
    make_relative_path(INCLUDEDIRSERVER, PGBINDIR, my_exec_path)
}

pub fn get_lib_path(my_exec_path: &str) -> String {
    make_relative_path(LIBDIR, PGBINDIR, my_exec_path)
}

pub fn get_pkglib_path(my_exec_path: &str) -> String {
    make_relative_path(PKGLIBDIR, PGBINDIR, my_exec_path)
}

pub fn get_locale_path(my_exec_path: &str) -> String {
    make_relative_path(LOCALEDIR, PGBINDIR, my_exec_path)
}

pub fn get_doc_path(my_exec_path: &str) -> String {
    make_relative_path(DOCDIR, PGBINDIR, my_exec_path)
}

pub fn get_html_path(my_exec_path: &str) -> String {
    make_relative_path(HTMLDIR, PGBINDIR, my_exec_path)
}

pub fn get_man_path(my_exec_path: &str) -> String {
    make_relative_path(MANDIR, PGBINDIR, my_exec_path)
}

// path.c:1022 get_home_path (Unix arm): $HOME when set and non-empty, else
// the passwd entry of the effective uid. Windows' APPDATA arm is not built.
pub fn get_home_path() -> Option<String> {
    if let Some(home) = std::env::var_os("HOME") {
        if !home.is_empty() {
            return Some(truncate_maxpgpath(&home.to_string_lossy()).to_string());
        }
    }
    home_from_passwd()
}

#[cfg(not(target_family = "wasm"))]
fn home_from_passwd() -> Option<String> {
    let mut pwbuf: libc::passwd = unsafe { core::mem::zeroed() };
    let mut buf = [0 as libc::c_char; 1024];
    let mut pw: *mut libc::passwd = core::ptr::null_mut();
    // SAFETY: getpwuid_r writes only into pwbuf/buf/pw, all live for the call;
    // pw_dir then points into buf, which outlives the copy below.
    let rc = unsafe {
        libc::getpwuid_r(libc::geteuid(), &mut pwbuf, buf.as_mut_ptr(), buf.len(), &mut pw)
    };
    if rc != 0 || pw.is_null() {
        return None;
    }
    // SAFETY: pw is non-NULL and pw_dir is a NUL-terminated string inside buf.
    let dir = unsafe { CStr::from_ptr((*pw).pw_dir) }.to_string_lossy();
    Some(truncate_maxpgpath(&dir).to_string())
}

// wasm32: WASI has no uids or passwd database — the lookup fails as
// getpwuid_r would on a system without the entry.
#[cfg(target_family = "wasm")]
fn home_from_passwd() -> Option<String> {
    None
}

/// Which `errcode()` exec.c's `log_error` macro attaches to a LOG report
/// (exec.c:54-70: `ereport(LOG, (errcodefn, errmsg(...)))` in the backend,
/// `pg_log_error` in frontends). The message text is passed alongside with
/// `%m` already expanded, and errno still holds the failing call's value
/// when the callback runs so `errcode_for_file_access()` classifies it
/// exactly as C's ereport does from its saved errno.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecLogCode {
    /// `errcode(ERRCODE_WRONG_OBJECT_TYPE)`
    WrongObjectType,
    /// `errcode(ERRCODE_UNDEFINED_FILE)`
    UndefinedFile,
    /// `errcode_for_file_access()`
    FileAccess,
    /// `errcode(ERRCODE_SYSTEM_ERROR)`
    SystemError,
    /// `errcode(ERRCODE_NO_DATA)`
    NoData,
}

impl ExecLogCode {
    /// The fixed SQLSTATE of the `errcode(...)` variants; `None` for
    /// `errcode_for_file_access()`, which ereport derives from errno.
    pub fn sqlstate(self) -> Option<[u8; 5]> {
        match self {
            ExecLogCode::WrongObjectType => Some(*b"42809"),
            ExecLogCode::UndefinedFile => Some(*b"58P01"),
            ExecLogCode::FileAccess => None,
            ExecLogCode::SystemError => Some(*b"58000"),
            ExecLogCode::NoData => Some(*b"02000"),
        }
    }
}

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    // SAFETY: writing the calling thread's errno slot.
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    // SAFETY: writing the calling thread's errno slot.
    unsafe {
        *libc::__errno_location() = value;
    }
}

/// `%m` as elog expands it: strerror(errnum).
fn strerror(errnum: i32) -> String {
    // SAFETY: strerror returns a static (or thread-local) NUL-terminated
    // string for any errnum; copied out immediately.
    unsafe { CStr::from_ptr(libc::strerror(errnum)) }
        .to_string_lossy()
        .into_owned()
}

/// C's `log_error(errcodefn, fmt": %m", ...)` shape: format with errno's
/// strerror, then hand the report to the caller's LOG sink with that same
/// errno in place (ereport reads it for errcode_for_file_access).
fn log_error_errno(
    log_error: &mut impl FnMut(ExecLogCode, String),
    code: ExecLogCode,
    errnum: i32,
    prefix: String,
) {
    let msg = format!("{prefix}: {}", strerror(errnum));
    set_errno(errnum);
    log_error(code, msg);
}

pub fn validate_exec(path: &str) -> i32 {
    let Ok(cpath) = CString::new(path.as_bytes()) else {
        set_errno(libc::ENOENT);
        return -1;
    };
    let mut buf: libc::stat = unsafe { core::mem::zeroed() };
    // SAFETY: cpath is NUL-terminated; buf is a plain-data out parameter.
    if unsafe { libc::stat(cpath.as_ptr(), &mut buf) } < 0 {
        return -1;
    }
    if (buf.st_mode & libc::S_IFMT) != libc::S_IFREG {
        // exec.c:124: POSIX has no "not a regular file" errno; EISDIR for
        // a directory, else EPERM (most likely a device special file).
        set_errno(if (buf.st_mode & libc::S_IFMT) == libc::S_IFDIR {
            libc::EISDIR
        } else {
            libc::EPERM
        });
        return -1;
    }
    // SAFETY: cpath is NUL-terminated; access() reads it only.
    let is_r = unsafe { libc::access(cpath.as_ptr(), libc::R_OK) } == 0;
    let is_x = unsafe { libc::access(cpath.as_ptr(), libc::X_OK) } == 0;
    // access() set errno if it returned -1.
    match (is_x, is_r) {
        (true, true) => 0,
        (true, false) => -2,
        (false, _) => -1,
    }
}

// exec.c:251: realpath failure is reported at LOG level (errcode_for_file_access,
// "%m") before -1; the Err payload only carries the same text for callers.
fn normalize_exec_path(
    path: &str,
    log_error: &mut impl FnMut(ExecLogCode, String),
) -> Result<String, String> {
    let resolved = match CString::new(path.as_bytes()) {
        // SAFETY: realpath(p, NULL) mallocs the result; freed below after copy.
        Ok(cpath) => unsafe { libc::realpath(cpath.as_ptr(), core::ptr::null_mut()) },
        Err(_) => {
            set_errno(libc::ENOENT);
            core::ptr::null_mut()
        }
    };
    if resolved.is_null() {
        let errnum = errno();
        let prefix = format!("could not resolve path \"{path}\" to absolute form");
        log_error_errno(log_error, ExecLogCode::FileAccess, errnum, prefix.clone());
        return Err(format!("{prefix}: {}", strerror(errnum)));
    }
    // SAFETY: realpath returned a valid NUL-terminated string.
    let mut out = unsafe { CStr::from_ptr(resolved) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: resolved was malloc'd by realpath.
    unsafe { libc::free(resolved.cast()) };
    out.truncate(out.len().min(MAXPGPATH - 1));
    Ok(out)
}

// exec.c:164 find_my_exec. Every failure leg reports at LOG level through
// `log_error` (exec.c's log_error macro — ereport(LOG, (errcode, errmsg))
// in the backend) BEFORE the -1 return; the Err payload repeats the text
// for callers that want it. The callback is passed in so this crate stays
// free of the elog dependency.
pub fn find_my_exec(
    argv0: &str,
    mut log_error: impl FnMut(ExecLogCode, String),
) -> Result<String, String> {
    let retpath = &argv0[..argv0.len().min(MAXPGPATH - 1)];

    // wasm32: the running module is not a file in the guest namespace (the
    // host runtime loaded it) — PATH search and X_OK probes are meaningless.
    // A synthetic absolute path keeps my_exec_path non-empty for the
    // pkglib/share derivations, which the PGRUST_* dir overrides control on
    // this target anyway.
    #[cfg(target_family = "wasm")]
    {
        let _ = &mut log_error;
        return Ok(if retpath.starts_with('/') {
            retpath.to_string()
        } else {
            format!("/{retpath}")
        });
    }

    if first_dir_separator(retpath).is_some() {
        if validate_exec(retpath) == 0 {
            return normalize_exec_path(retpath, &mut log_error);
        }
        // exec.c:173
        let errnum = errno();
        let prefix = format!("invalid binary \"{retpath}\"");
        log_error_errno(&mut log_error, ExecLogCode::WrongObjectType, errnum, prefix.clone());
        return Err(format!("{prefix}: {}", strerror(errnum)));
    }

    if let Ok(path) = std::env::var("PATH") {
        if !path.is_empty() {
            let mut startp = 0usize;
            loop {
                let endp = match first_path_var_separator(&path[startp..]) {
                    Some(off) => startp + off,
                    None => path.len(),
                };
                let seg = &path[startp..endp.min(startp + (MAXPGPATH - 1))];
                let candidate = canonicalize_path(&join_path_components(seg, argv0));
                match validate_exec(&candidate) {
                    0 => return normalize_exec_path(&candidate, &mut log_error),
                    // exec.c:211: found but disqualified
                    -2 => {
                        let errnum = errno();
                        log_error_errno(
                            &mut log_error,
                            ExecLogCode::WrongObjectType,
                            errnum,
                            format!("could not read binary \"{candidate}\""),
                        );
                    }
                    _ => {}
                }
                if endp >= path.len() {
                    break;
                }
                startp = endp + 1;
            }
        }
    }

    // exec.c:224
    let msg = format!("could not find a \"{argv0}\" to execute");
    log_error(ExecLogCode::UndefinedFile, msg.clone());
    Err(msg)
}

/// find_other_exec's two failure returns (exec.c:310).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindOtherExecError {
    /// -1: this executable or the target could not be located, validated
    /// or run (the reason was reported through `log_error`).
    NotFound,
    /// -2: the target ran but its `-V` line was not `versionstr`.
    WrongVersion,
}

// exec.c:310: find another program in our binary's directory, then make
// sure it is the proper version (its `-V` output must equal versionstr,
// newline included — PG_BACKEND_VERSIONSTR carries one).
#[cfg(not(target_family = "wasm"))]
pub fn find_other_exec(
    argv0: &str,
    target: &str,
    versionstr: &str,
    mut log_error: impl FnMut(ExecLogCode, String),
) -> Result<String, FindOtherExecError> {
    let mut retpath =
        find_my_exec(argv0, &mut log_error).map_err(|_| FindOtherExecError::NotFound)?;

    /* Trim off program name and keep just directory */
    retpath.truncate(last_dir_separator(&retpath).unwrap_or(0));
    let mut retpath = canonicalize_path(&retpath);

    /* Now append the other program's name */
    retpath.push('/');
    retpath.push_str(target);
    let retpath = truncate_maxpgpath(&retpath).to_string();

    if validate_exec(&retpath) != 0 {
        return Err(FindOtherExecError::NotFound);
    }

    let cmd = truncate_maxpgpath(&format!("\"{retpath}\" -V")).to_string();

    let line = pipe_read_line(&cmd, &mut log_error).ok_or(FindOtherExecError::NotFound)?;

    if line != versionstr {
        return Err(FindOtherExecError::WrongVersion);
    }

    Ok(retpath)
}

// exec.c:353: execute a command in a pipe and read the first line from it
// (pg_get_line semantics: up to and including the newline; None when the
// command produced nothing or could not be run).
#[cfg(not(target_family = "wasm"))]
pub fn pipe_read_line(cmd: &str, log_error: &mut impl FnMut(ExecLogCode, String)) -> Option<String> {
    // SAFETY: fflush(NULL) flushes every open output stream; no pointers.
    unsafe { libc::fflush(core::ptr::null_mut()) };

    set_errno(0);
    let Ok(ccmd) = CString::new(cmd) else {
        set_errno(libc::EINVAL);
        log_error_errno(
            &mut *log_error,
            ExecLogCode::SystemError,
            libc::EINVAL,
            format!("could not execute command \"{cmd}\""),
        );
        return None;
    };
    // SAFETY: both arguments are NUL-terminated; the stream is pclose'd below.
    let pipe_cmd = unsafe { libc::popen(ccmd.as_ptr(), c"r".as_ptr()) };
    if pipe_cmd.is_null() {
        let errnum = errno();
        log_error_errno(
            &mut *log_error,
            ExecLogCode::SystemError,
            errnum,
            format!("could not execute command \"{cmd}\""),
        );
        return None;
    }

    /* Make sure popen() didn't change errno */
    set_errno(0);
    let mut bytes = Vec::new();
    loop {
        // SAFETY: pipe_cmd is an open stream owned by this function.
        let ch = unsafe { libc::fgetc(pipe_cmd) };
        if ch == libc::EOF {
            break;
        }
        bytes.push(ch as u8);
        if ch == i32::from(b'\n') {
            break;
        }
    }
    let line = if bytes.is_empty() {
        // SAFETY: pipe_cmd is an open stream.
        if unsafe { libc::ferror(pipe_cmd) } != 0 {
            let errnum = errno();
            log_error_errno(
                &mut *log_error,
                ExecLogCode::FileAccess,
                errnum,
                format!("could not read from command \"{cmd}\""),
            );
        } else {
            log_error(
                ExecLogCode::NoData,
                format!("no data was returned by command \"{cmd}\""),
            );
        }
        None
    } else {
        Some(String::from_utf8_lossy(&bytes).into_owned())
    };

    let _ = pclose_check(pipe_cmd, log_error);

    line
}

// exec.c:391: pclose() plus useful error reporting.
#[cfg(not(target_family = "wasm"))]
pub fn pclose_check(stream: *mut libc::FILE, log_error: &mut impl FnMut(ExecLogCode, String)) -> i32 {
    // SAFETY: stream came from popen and is closed exactly once here.
    let exitstatus = unsafe { libc::pclose(stream) };

    if exitstatus == 0 {
        return 0; /* all is well */
    }

    if exitstatus == -1 {
        /* pclose() itself failed, and hopefully set errno */
        let errnum = errno();
        log_error_errno(&mut *log_error, ExecLogCode::SystemError, errnum, "pclose() failed".to_string());
    } else {
        log_error(ExecLogCode::SystemError, wait_error::wait_result_to_str(exitstatus));
    }
    exitstatus
}

/// The backend's `PG_TEXTDOMAIN("postgres")` (c.h: domain "-" PG_MAJORVERSION).
pub const PG_TEXTDOMAIN_POSTGRES: &str = "postgres-18";

// exec.c set_pglocale_pgservice: locate this executable (its find_my_exec
// reports at LOG level and the failure is otherwise silent), then set
// PGSYSCONFDIR for libpq without overriding an existing setting. The
// ENABLE_NLS arm (bindtextdomain/PGLOCALEDIR) is not built.
pub fn set_pglocale_pgservice(
    argv0: &str,
    app: &str,
    log_error: impl FnMut(ExecLogCode, String),
) {
    /* don't set LC_ALL in the backend */
    // wasm32-wasip1 has no setlocale (libc exposes no LC_ALL there); the
    // module only ever runs as the backend, which skips this arm anyway.
    #[cfg(not(target_family = "wasm"))]
    if app != PG_TEXTDOMAIN_POSTGRES {
        // SAFETY: process-startup call on the main thread, before any
        // locale-dependent library state exists.
        unsafe {
            libc::setlocale(libc::LC_ALL, c"".as_ptr());
        }
    }
    #[cfg(target_family = "wasm")]
    let _ = app;

    let Ok(my_exec_path) = find_my_exec(argv0, log_error) else {
        return;
    };

    if std::env::var_os("PGSYSCONFDIR").is_none() {
        let path = get_etc_path(&my_exec_path);
        /* set for libpq to use */
        if let Ok(cpath) = CString::new(path) {
            // SAFETY: process-startup call on the main thread; setenv(…, 0)
            // never overwrites an existing value.
            unsafe {
                libc::setenv(c"PGSYSCONFDIR".as_ptr(), cpath.as_ptr(), 0);
            }
        }
    }
}
