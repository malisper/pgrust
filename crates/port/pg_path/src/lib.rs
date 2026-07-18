//! src/port/path.c (Unix arms) + find_my_exec from src/common/exec.c.

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
    let mut path: Vec<u8> = input.as_bytes().to_vec();
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
        return String::new();
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
    String::from_utf8(out).expect("components sliced on ASCII '/' boundaries")
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

pub fn validate_exec(path: &str) -> i32 {
    let Ok(cpath) = CString::new(path.as_bytes()) else {
        return -1;
    };
    let mut buf: libc::stat = unsafe { core::mem::zeroed() };
    // SAFETY: cpath is NUL-terminated; buf is a plain-data out parameter.
    if unsafe { libc::stat(cpath.as_ptr(), &mut buf) } < 0 {
        return -1;
    }
    if (buf.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return -1;
    }
    // SAFETY: cpath is NUL-terminated; access() reads it only.
    let is_r = unsafe { libc::access(cpath.as_ptr(), libc::R_OK) } == 0;
    let is_x = unsafe { libc::access(cpath.as_ptr(), libc::X_OK) } == 0;
    match (is_x, is_r) {
        (true, true) => 0,
        (true, false) => -2,
        (false, _) => -1,
    }
}

fn normalize_exec_path(path: &str) -> Result<String, String> {
    let cpath = CString::new(path.as_bytes())
        .map_err(|_| format!("could not resolve path \"{path}\" to absolute form"))?;
    // SAFETY: realpath(p, NULL) mallocs the result; freed below after copy.
    let resolved = unsafe { libc::realpath(cpath.as_ptr(), core::ptr::null_mut()) };
    if resolved.is_null() {
        return Err(format!(
            "could not resolve path \"{path}\" to absolute form"
        ));
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

// C's -2 leg logs and keeps scanning PATH; `log_error` is exec.c's LOG-level
// macro passed in so this crate stays free of the elog dependency.
pub fn find_my_exec(argv0: &str, mut log_error: impl FnMut(String)) -> Result<String, String> {
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
            return normalize_exec_path(retpath);
        }
        return Err(format!("invalid binary \"{retpath}\""));
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
                    0 => return normalize_exec_path(&candidate),
                    -2 => log_error(format!("could not read binary \"{candidate}\"")),
                    _ => {}
                }
                if endp >= path.len() {
                    break;
                }
                startp = endp + 1;
            }
        }
    }

    Err(format!("could not find a \"{argv0}\" to execute"))
}
