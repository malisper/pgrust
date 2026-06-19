//! Faithful (non-Windows) ports of the `src/port/path.c` lexical path helpers
//! and the `src/common/exec.c` executable-locating routines that the standalone
//! boot prelude reaches via `InitStandaloneProcess` (`miscinit.c:203`).
//!
//! These are pure string/`stat`/`realpath` leaf functions with no dedicated
//! owner crate in this tree yet; they are homed here (the process-init crate the
//! boot path already routes through, next to `startup_paths`) and the
//! `resolve_standalone_paths` seam is installed from [`crate::init_seams`].
//!
//! Every body mirrors its C source on the Unix build (`IS_DIR_SEP` == `/`,
//! `skip_drive` == identity, `make_native_path` == identity, no `.exe` suffix);
//! the Windows-only arms reduce away exactly as documented in the C source.

use std::ffi::{CStr, CString};

use types_error::{PgError, PgResult, FATAL};

/// PostgreSQL's `MAXPGPATH` (`pg_config_manual.h`).
const MAXPGPATH: usize = 1024;

// ---------------------------------------------------------------------------
// Compiled-in installation directories (pg_config.h)
//
// These mirror the configure-time `PKGLIBDIR` / `PGBINDIR` literals that
// `make_relative_path` relativizes against the running executable. They can be
// overridden at build time; otherwise they fall back to PostgreSQL's documented
// `/usr/local/pgsql/...` defaults (identical to the rendering the rest of the
// tree uses).
// ---------------------------------------------------------------------------

const DEFAULT_PGBINDIR: &str = "/usr/local/pgsql/bin";
const DEFAULT_PKGLIBDIR: &str = "/usr/local/pgsql/lib";
const DEFAULT_PGSHAREDIR: &str = "/usr/local/pgsql/share";

#[inline]
fn configured_pgbindir() -> &'static str {
    option_env!("PGRUST_PGBINDIR").unwrap_or(DEFAULT_PGBINDIR)
}

#[inline]
fn configured_pkglibdir() -> &'static str {
    option_env!("PGRUST_PKGLIBDIR").unwrap_or(DEFAULT_PKGLIBDIR)
}

#[inline]
fn configured_sharedir() -> &'static str {
    option_env!("PGRUST_PGSHAREDIR").unwrap_or(DEFAULT_PGSHAREDIR)
}

// ---------------------------------------------------------------------------
// Separator predicates (the IS_DIR_SEP / IS_PATH_VAR_SEP macros, Unix build)
// ---------------------------------------------------------------------------

/// `IS_DIR_SEP(ch)` (`src/include/port.h`): `/` on the Unix build.
#[inline]
fn is_dir_sep(ch: u8) -> bool {
    ch == b'/'
}

/// `IS_PATH_VAR_SEP(ch)` (`src/include/port.h`): `:` on the Unix build.
#[inline]
fn is_path_var_sep(ch: u8) -> bool {
    ch == b':'
}

// ---------------------------------------------------------------------------
// skip_drive / separator scans (path.c)
// ---------------------------------------------------------------------------

/// `first_dir_separator(filename)` (`path.c`): byte offset of the first `/`
/// (after the no-op drive skip), or `None`.
fn first_dir_separator(filename: &[u8]) -> Option<usize> {
    filename.iter().position(|&b| is_dir_sep(b))
}

/// `first_path_var_separator(pathlist)` (`path.c`): byte offset of the first
/// `PATH`-variable separator (`:`), or `None`.
fn first_path_var_separator(pathlist: &[u8]) -> Option<usize> {
    pathlist.iter().position(|&b| is_path_var_sep(b))
}

// ---------------------------------------------------------------------------
// join_path_components (path.c)
// ---------------------------------------------------------------------------

/// `join_path_components(ret_path, head, tail)` (`path.c`): join `head` and
/// `tail`, separating with a single `/` only when `head` (after the no-op drive
/// skip) is non-empty. When `tail` is empty the result is just `head`. The full
/// `.`/`..` simplification is left to [`canonicalize_path`], exactly as in C.
fn join_path_components(head: &str, tail: &str) -> String {
    let mut ret = head.to_string();
    if !tail.is_empty() {
        // "only separate with slash if head wasn't empty" — skip_drive(head)
        // is identity on Unix, so the test is just "head non-empty".
        if !head.is_empty() {
            ret.push('/');
        }
        ret.push_str(tail);
    }
    ret
}

// ---------------------------------------------------------------------------
// trim_trailing_separator / trim_directory (path.c file-static helpers)
// ---------------------------------------------------------------------------

/// `trim_trailing_separator(path)` (`path.c`): drop trailing slashes but never
/// the leading slash (C's `p > path` guard keeps byte 0).
fn trim_trailing_separator(path: &mut Vec<u8>) {
    if path.is_empty() {
        return;
    }
    let mut end = path.len();
    while end > 1 && is_dir_sep(path[end - 1]) {
        end -= 1;
    }
    path.truncate(end);
}

/// `trim_directory(path)` (`path.c`): remove any trailing slashes, the last
/// pathname component, and the slash(es) just ahead of it — but never the
/// leading slash. Mutates in place (the C buffer-truncation analog). Returns the
/// new length (C returns the interior `char *`; the in-place form only needs the
/// truncation point, which the callers re-derive from the buffer).
fn trim_directory(path: &mut Vec<u8>) {
    if path.is_empty() {
        return;
    }
    // p = path + strlen(path) - 1
    let mut p = path.len() - 1;
    // back up over trailing slash(es): while IS_DIR_SEP(*p) && p > path
    while is_dir_sep(path[p]) && p > 0 {
        p -= 1;
    }
    // back up over the directory name: while !IS_DIR_SEP(*p) && p > path
    while !is_dir_sep(path[p]) && p > 0 {
        p -= 1;
    }
    // remove multiple slashes before the name: while p > path && IS_DIR_SEP(*(p-1))
    while p > 0 && is_dir_sep(path[p - 1]) {
        p -= 1;
    }
    // don't erase a leading slash: if p == path && IS_DIR_SEP(*p) then p++
    if p == 0 && is_dir_sep(path[0]) {
        p += 1;
    }
    path.truncate(p);
}

// ---------------------------------------------------------------------------
// canonicalize_path (path.c) — faithful port of the C state machine
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum CanonState {
    AbsoluteInit,
    AbsoluteWithDepth,
    RelativeInit,
    RelativeWithDepth,
    RelativeWithParentRef,
}

/// `canonicalize_path(path)` (`path.c`, non-Windows): clean up a path lexically
/// — remove the trailing slash (but not a leading slash), collapse duplicate
/// adjacent separators, drop `.` components (unless the path reduces to `.`),
/// and resolve `..` where possible. Faithful port of the C in-place state
/// machine onto a `Vec<u8>` working buffer; `skip_drive` is identity on Unix.
fn canonicalize_path(input: &str) -> String {
    // Working buffer mirrors C's in-place char[MAXPGPATH] mutation.
    let mut path: Vec<u8> = input.as_bytes().to_vec();

    // (Win32 debackslash / trailing-quote handling is compiled out on Unix.)

    // Remove the trailing slash, never the leading one.
    trim_trailing_separator(&mut path);

    // Remove duplicate adjacent separators.
    {
        let mut out: Vec<u8> = Vec::with_capacity(path.len());
        let mut was_sep = false;
        let mut i = 0;
        while i < path.len() {
            // Handle many adjacent slashes, like "/a///b".
            while i < path.len() && path[i] == b'/' && was_sep {
                i += 1;
            }
            if i < path.len() {
                out.push(path[i]);
                was_sep = path[i] == b'/';
                i += 1;
            }
        }
        path = out;
    }

    // spath = skip_drive(path) — identity on Unix.
    if path.is_empty() {
        // empty path is returned as-is
        return String::new();
    }

    // The "parsed"/"unparse" pointers in C index into spath (== path on Unix).
    // We rebuild the resolved component list and reconstruct the output, which
    // is observationally identical to C's in-place overwrite.
    let absolute = path[0] == b'/';

    // Split the body (after a leading slash for absolute paths) into '/'-delimited
    // components, exactly as C's unparse loop does (adjacent separators already
    // eliminated above, so no empty interior components arise except possibly a
    // leading empty one which we skip via the absolute flag).
    let body: &[u8] = if absolute { &path[1..] } else { &path[..] };

    // dirs holds the resolved component stack; for the relative-parent-ref case
    // we keep ".." entries explicitly, matching C's behavior.
    let mut dirs: Vec<&[u8]> = Vec::new();
    let mut state = if absolute {
        CanonState::AbsoluteInit
    } else {
        CanonState::RelativeInit
    };
    let mut pathdepth: i32 = 0;

    let mut comp_start = 0usize;
    let comps = {
        let mut v: Vec<&[u8]> = Vec::new();
        if !body.is_empty() {
            let mut i = 0;
            while i <= body.len() {
                if i == body.len() || body[i] == b'/' {
                    v.push(&body[comp_start..i]);
                    comp_start = i + 1;
                }
                i += 1;
            }
        }
        v
    };

    for comp in comps {
        // Adjacent separators were eliminated, but a trailing slash could have
        // produced one trailing empty component; C's loop terminates on '\0' so
        // an empty trailing component does not occur. Guard defensively.
        if comp.is_empty() {
            continue;
        }
        // Ignore "." components in all cases.
        if comp == b"." {
            continue;
        }
        let is_double_dot = comp == b"..";

        match state {
            CanonState::AbsoluteInit => {
                // We can ignore ".." immediately after /.
                if !is_double_dot {
                    dirs.push(comp);
                    state = CanonState::AbsoluteWithDepth;
                    pathdepth += 1;
                }
            }
            CanonState::AbsoluteWithDepth => {
                if is_double_dot {
                    // Remove last parsed dir.
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
                    dirs.push(comp); // irreducible ".."
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
                        // If output is now empty -> back to INIT; if we still
                        // have a leading ".." (e.g. "../dir/..") -> PARENT_REF.
                        if dirs.is_empty() {
                            state = CanonState::RelativeInit;
                        } else {
                            state = CanonState::RelativeWithParentRef;
                        }
                    }
                } else {
                    dirs.push(comp);
                    pathdepth += 1;
                }
            }
            CanonState::RelativeWithParentRef => {
                if is_double_dot {
                    dirs.push(comp); // next irreducible ".."
                } else {
                    dirs.push(comp);
                    state = CanonState::RelativeWithDepth;
                    pathdepth = 1;
                }
            }
        }
    }

    // Reconstruct the output string.
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

    // If the output path is empty at this point, insert ".".
    // C: `if (parsed == spath) *parsed++ = '.';` — i.e. nothing was emitted
    // (for an absolute path, spath still has its leading slash so this never
    // fires; for a relative path it fires when no components survived).
    if !absolute && out.is_empty() {
        out.push(b'.');
    }

    // SAFETY: input was valid UTF-8 and we only sliced on ASCII '/'.
    String::from_utf8(out).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// dir_strcmp (path.c)
// ---------------------------------------------------------------------------

/// `dir_strcmp(s1, s2)` (`path.c`, non-Windows): `strcmp` except any two
/// directory-separator bytes compare equal. Returns 0 on equality.
fn dir_strcmp(s1: &[u8], s2: &[u8]) -> i32 {
    let n = s1.len().min(s2.len());
    for i in 0..n {
        let a = s1[i];
        let b = s2[i];
        if a != b && !(is_dir_sep(a) && is_dir_sep(b)) {
            return a as i32 - b as i32;
        }
    }
    if s1.len() > n {
        1
    } else if s2.len() > n {
        -1
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// make_relative_path (path.c) -> get_pkglib_path (path.c)
// ---------------------------------------------------------------------------

/// `make_relative_path(ret_path, target_path, bin_path, my_exec_path)`
/// (`path.c`): make `target_path` relative to the actual binary location, to
/// support relocation of installation trees. Faithful port of the C algorithm.
fn make_relative_path(target_path: &str, bin_path: &str, my_exec_path: &str) -> String {
    let target = target_path.as_bytes();
    let bin = bin_path.as_bytes();

    // Determine the common prefix, requiring it to end on a directory separator.
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
        // no common prefix
        return canonicalize_path(target_path);
    }
    let tail_len = bin.len() - prefix_len;

    // ret_path = my_exec_path without the executable name, canonicalized.
    let mut ret: Vec<u8> = my_exec_path.as_bytes().to_vec();
    if ret.len() >= MAXPGPATH {
        ret.truncate(MAXPGPATH - 1);
    }
    trim_directory(&mut ret); // remove the executable name
    let canon = canonicalize_path(&String::from_utf8_lossy(&ret));
    let ret = canon.as_bytes();

    // Tail match?
    // tail_start = strlen(ret_path) - tail_len  (signed in C)
    let tail_start = ret.len() as isize - tail_len as isize;
    if tail_start > 0 {
        let ts = tail_start as usize;
        if is_dir_sep(ret[ts - 1]) && dir_strcmp(&ret[ts..], &bin[prefix_len..]) == 0 {
            // ret_path[tail_start] = '\0' ; trim_trailing_separator ; join ; canon
            let mut head: Vec<u8> = ret[..ts].to_vec();
            trim_trailing_separator(&mut head);
            let head_str = String::from_utf8_lossy(&head).into_owned();
            // target_path + prefix_len
            let target_tail = &target_path[prefix_len..];
            let joined = join_path_components(&head_str, target_tail);
            return canonicalize_path(&joined);
        }
    }

    // no_match: ret_path = target_path; canonicalize_path(ret_path);
    canonicalize_path(target_path)
}

/// `get_pkglib_path(my_exec_path, ret_path)` (`path.c`):
/// `make_relative_path(ret_path, PKGLIBDIR, PGBINDIR, my_exec_path)`.
fn get_pkglib_path(my_exec_path: &str) -> String {
    make_relative_path(configured_pkglibdir(), configured_pgbindir(), my_exec_path)
}

/// `get_share_path(my_exec_path, ret_path)` (`path.c`):
/// `make_relative_path(ret_path, PGSHAREDIR, PGBINDIR, my_exec_path)`. Derives
/// the installation's `share` directory from the running executable so a
/// relocated install still finds its data files (timezonesets, the tzdb, etc.).
pub fn get_share_path(my_exec_path: &str) -> String {
    make_relative_path(configured_sharedir(), configured_pgbindir(), my_exec_path)
}

// ---------------------------------------------------------------------------
// validate_exec / normalize_exec_path / find_my_exec (common/exec.c)
// ---------------------------------------------------------------------------

/// `validate_exec(path)` (`common/exec.c`, non-Windows): ensure `path` exists,
/// is a regular file, and is both readable and executable. Returns
/// `0` (ok), `-1` (not a candidate), or `-2` (found but disqualified — not
/// readable). `errno` semantics from C are not surfaced (the callers only branch
/// on the integer code on the Unix build).
fn validate_exec(path: &str) -> i32 {
    let cpath = match CString::new(path.as_bytes()) {
        Ok(c) => c,
        Err(_) => return -1, // embedded NUL: can't stat, not a candidate
    };

    // stat(path, &buf)
    // SAFETY: cpath is a valid NUL-terminated C string; buf is zeroed.
    let mut buf: libc::stat = unsafe { core::mem::zeroed() };
    let rc = unsafe { libc::stat(cpath.as_ptr(), &mut buf) };
    if rc < 0 {
        return -1;
    }

    // !S_ISREG(buf.st_mode) -> not a regular file
    if (buf.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return -1;
    }

    // is_r = access(path, R_OK) == 0 ; is_x = access(path, X_OK) == 0
    let is_r = unsafe { libc::access(cpath.as_ptr(), libc::R_OK) } == 0;
    let is_x = unsafe { libc::access(cpath.as_ptr(), libc::X_OK) } == 0;

    // return is_x ? (is_r ? 0 : -2) : -1;
    if is_x {
        if is_r {
            0
        } else {
            -2
        }
    } else {
        -1
    }
}

/// `normalize_exec_path(path)` (`common/exec.c`): resolve `path` to absolute
/// form via `realpath(3)`. Returns the resolved path, or `Err` if it cannot be
/// resolved.
fn normalize_exec_path(path: &str) -> PgResult<String> {
    let cpath = CString::new(path.as_bytes())
        .map_err(|_| PgError::new(FATAL, format!("invalid path \"{path}\"")))?;

    // pg_realpath(path) == realpath(path, NULL)
    // SAFETY: cpath is valid; realpath(.., NULL) mallocs the result, which we
    // copy out and free.
    let resolved = unsafe { libc::realpath(cpath.as_ptr(), core::ptr::null_mut()) };
    if resolved.is_null() {
        return Err(PgError::new(
            FATAL,
            format!("could not resolve path \"{path}\" to absolute form"),
        ));
    }
    // SAFETY: realpath returned a valid NUL-terminated C string.
    let out = unsafe { CStr::from_ptr(resolved) }
        .to_string_lossy()
        .into_owned();
    unsafe { libc::free(resolved as *mut libc::c_void) };

    // strlcpy into MAXPGPATH.
    let out = if out.len() >= MAXPGPATH {
        out[..MAXPGPATH - 1].to_string()
    } else {
        out
    };
    Ok(out)
}

/// `find_my_exec(argv0, retpath)` (`common/exec.c`): find an absolute path to
/// this program's executable. Returns the resolved path, or `Err` if it cannot
/// be located (the C `return -1` legs, which the caller turns into `elog(FATAL)`).
fn find_my_exec(argv0: &str) -> PgResult<String> {
    // strlcpy(retpath, argv0, MAXPGPATH)
    let retpath = if argv0.len() >= MAXPGPATH {
        &argv0[..MAXPGPATH - 1]
    } else {
        argv0
    };

    // If argv0 contains a separator, PATH wasn't used.
    if first_dir_separator(retpath.as_bytes()).is_some() {
        if validate_exec(retpath) == 0 {
            return normalize_exec_path(retpath);
        }
        return Err(PgError::new(
            FATAL,
            format!("invalid binary \"{retpath}\""),
        ));
    }

    // No explicit path: search PATH.
    if let Ok(path) = std::env::var("PATH") {
        if !path.is_empty() {
            let bytes = path.as_bytes();
            let mut startp = 0usize;
            loop {
                // endp = first_path_var_separator(startp); if none -> end
                let rest = &bytes[startp..];
                let endp = match first_path_var_separator(rest) {
                    Some(off) => startp + off,
                    None => bytes.len(),
                };

                // strlcpy(retpath, startp, Min(endp - startp + 1, MAXPGPATH))
                // -> the PATH entry [startp, endp), truncated to MAXPGPATH-1.
                let seg_end = (endp).min(startp + (MAXPGPATH - 1));
                let seg = &path[startp..seg_end];

                // join_path_components(retpath, retpath, argv0); canonicalize_path(retpath);
                let joined = join_path_components(seg, argv0);
                let candidate = canonicalize_path(&joined);

                match validate_exec(&candidate) {
                    0 => return normalize_exec_path(&candidate),
                    -1 => { /* not a candidate, keep looking */ }
                    -2 => {
                        return Err(PgError::new(
                            FATAL,
                            format!("could not read binary \"{candidate}\""),
                        ));
                    }
                    _ => {}
                }

                // do { ... } while (*endp);
                if endp >= bytes.len() {
                    break;
                }
                startp = endp + 1;
            }
        }
    }

    Err(PgError::new(
        FATAL,
        format!("could not find a \"{argv0}\" to execute"),
    ))
}

// ---------------------------------------------------------------------------
// resolve_standalone_paths — the InitStandaloneProcess path-computing tail
// ---------------------------------------------------------------------------

/// The path-computing tail of `InitStandaloneProcess(argv0)` (`miscinit.c:203`):
///
/// ```c
/// if (my_exec_path[0] == '\0')
///     if (find_my_exec(argv0, my_exec_path) < 0)
///         elog(FATAL, "%s: could not locate my own executable path", argv0);
/// if (pkglib_path[0] == '\0')
///     get_pkglib_path(my_exec_path, pkglib_path);
/// ```
///
/// `find_my_exec`'s own `-1` legs already log a specific FATAL; the
/// `InitStandaloneProcess` wrapper additionally maps a failure to the generic
/// "could not locate my own executable path" message, which we surface as the
/// returned `FATAL` error so the boot exits cleanly.
pub fn resolve_standalone_paths(argv0: &str) -> PgResult<()> {
    use backend_utils_init_small::globals;

    // my_exec_path[0] == '\0' ?
    let cur = globals::my_exec_path();
    if cur[0] == 0 {
        let resolved = find_my_exec(argv0).map_err(|_| {
            PgError::new(
                FATAL,
                format!("{argv0}: could not locate my own executable path"),
            )
        })?;
        globals::set_my_exec_path(strlcpy_to_buf(&resolved));
    }

    // pkglib_path[0] == '\0' ?
    let cur_pkg = globals::pkglib_path();
    if cur_pkg[0] == 0 {
        let exec = globals::my_exec_path();
        let exec_str = cstr_buf_to_str(&exec);
        let pkglib = get_pkglib_path(&exec_str);
        globals::set_pkglib_path(strlcpy_to_buf(&pkglib));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// make_absolute_path (path.c) + the small port/libc seams miscinit reaches
// ---------------------------------------------------------------------------

/// `is_absolute_path(path)` (`port.h`, Unix): begins with `/`.
fn is_absolute_path(path: &str) -> bool {
    path.as_bytes().first() == Some(&b'/')
}

/// `make_absolute_path(path)` (`path.c`): if the path is relative, prepend the
/// current working directory; then canonicalize. C `ereport(ERROR)`s on
/// `getcwd`/`malloc` failure — here surfaced as `FATAL` (the backend leg).
pub fn make_absolute_path(path: &str) -> PgResult<String> {
    let joined = if is_absolute_path(path) {
        path.to_string()
    } else {
        // getcwd, growing the buffer on ERANGE.
        let cwd = std::env::current_dir().map_err(|e| {
            PgError::new(
                FATAL,
                format!("could not get current working directory: {e}"),
            )
        })?;
        let cwd = cwd.to_string_lossy().into_owned();
        // sprintf(new, "%s/%s", buf, path)
        format!("{cwd}/{path}")
    };
    // Make sure punctuation is canonical, too.
    Ok(canonicalize_path(&joined))
}

/// `first_dir_separator(filename)` (`path.c`): public byte-offset accessor used
/// by `load_libraries` (the `$libdir/plugins/` prefix decision).
pub fn first_dir_separator_pub(filename: &str) -> Option<usize> {
    first_dir_separator(filename.as_bytes())
}

/// `pstrdup(path); canonicalize_path(buf)` (`path.c`): public canonicalizing
/// accessor used by `commands/tablespace.c` (Unix-ify the offered LOCATION and
/// strip trailing slashes). Infallible in C beyond the `pstrdup` alloc.
pub fn canonicalize_path_pub(path: &str) -> PgResult<String> {
    Ok(canonicalize_path(path))
}

/// `is_absolute_path(path)` (`port.h`): public predicate accessor.
pub fn is_absolute_path_pub(path: &str) -> PgResult<bool> {
    Ok(is_absolute_path(path))
}

/// `canonicalize_path(path)` (`common/path.c`) seam adapter: the
/// `common_path_seams`/owned-`String` shape (canonicalization can change the
/// length, so the canonical form is returned). Faithful lexical non-Windows
/// port — the same body `commands/tablespace.c` reaches via
/// [`canonicalize_path_pub`], here exposed for the `dfmgr`/`extension`/
/// `variable`/`varlena` callers that take owned strings.
pub fn canonicalize_path_owned(path: String) -> String {
    canonicalize_path(&path)
}

/// `is_absolute_path(path)` (`common/path.c` and `port/path.c`, identical
/// non-Windows bodies): bare `bool` predicate accessor for the
/// `common_path_seams`/`port_path_seams` callers (`dfmgr`, `extension`,
/// `copyto`).
pub fn is_absolute_path_bool(path: &str) -> bool {
    is_absolute_path(path)
}

/// `path_is_prefix_of_path(path1, path2)` (`path.c`): true when `path1` is a
/// prefix of `path2` that ends on a directory boundary (i.e. `path2` continues
/// with a directory separator or the string ends). Pure string predicate.
pub fn path_is_prefix_of_path_pub(path1: &str, path2: &str) -> PgResult<bool> {
    let p1 = path1.as_bytes();
    let p2 = path2.as_bytes();
    let path1_len = p1.len();
    let starts_with = p2.len() >= path1_len && &p2[..path1_len] == p1;
    // IS_DIR_SEP(path2[path1_len]) || path2[path1_len] == '\0'
    let boundary = match p2.get(path1_len) {
        Some(&c) => is_dir_sep(c),
        None => true, // the C '\0' terminator
    };
    Ok(starts_with && boundary)
}

/// `pstrdup(path); get_parent_directory(buf)` (`path.c`): strip the last path
/// component (and the slash(es) just ahead of it), returning the parent
/// directory. Never erases a leading slash.
pub fn get_parent_directory_pub(path: &str) -> PgResult<String> {
    let mut buf: Vec<u8> = path.as_bytes().to_vec();
    trim_directory(&mut buf);
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// `getppid()` (libc): parent process id.
pub fn getppid() -> i32 {
    // SAFETY: getppid never fails and has no preconditions.
    unsafe { libc::getppid() }
}

/// `kill(pid, 0) == 0 || (errno != ESRCH && errno != EPERM)` — whether the PID
/// in a stale lock file appears to belong to a live process (`miscinit.c`'s
/// `CreateLockFile`).
pub fn pid_appears_live(pid: i32) -> bool {
    // SAFETY: kill(pid, 0) only probes signal-deliverability; no preconditions.
    let rc = unsafe { libc::kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno != libc::ESRCH && errno != libc::EPERM
}

/// `utime(path, NULL)` (`miscinit.c`): bump the socket lock file's times so a
/// `/tmp`-cleaner does not remove it. Errors are ignored (C casts to `(void)`).
pub fn touch_file_times(path: &str) {
    if let Ok(cpath) = CString::new(path.as_bytes()) {
        // SAFETY: cpath is a valid NUL-terminated C string; NULL times == now.
        unsafe {
            libc::utime(cpath.as_ptr(), core::ptr::null());
        }
    }
}

/// `strlcpy(buf, s, MAXPGPATH)` into a fresh `[u8; MAXPGPATH]` (NUL-terminated,
/// truncated to fit), mirroring the C fixed-buffer globals.
fn strlcpy_to_buf(s: &str) -> [u8; MAXPGPATH] {
    let mut buf = [0u8; MAXPGPATH];
    let bytes = s.as_bytes();
    let len = bytes.len().min(MAXPGPATH - 1);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

/// Read a C `char[MAXPGPATH]` buffer up to its first NUL as a `String`.
fn cstr_buf_to_str(buf: &[u8; MAXPGPATH]) -> String {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(MAXPGPATH);
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_basic() {
        assert_eq!(canonicalize_path("/usr/local//bin/"), "/usr/local/bin");
        assert_eq!(canonicalize_path("/usr/./local/bin"), "/usr/local/bin");
        assert_eq!(canonicalize_path("/usr/local/../bin"), "/usr/bin");
        assert_eq!(canonicalize_path("/../.."), "/");
        assert_eq!(canonicalize_path("a/b/../c"), "a/c");
        assert_eq!(canonicalize_path("../.."), "../..");
        assert_eq!(canonicalize_path("."), ".");
        assert_eq!(canonicalize_path("./foo"), "foo");
    }

    #[test]
    fn trim_directory_basic() {
        let mut p = b"/usr/local/bin/postgres".to_vec();
        trim_directory(&mut p);
        assert_eq!(p, b"/usr/local/bin");
        let mut q = b"/postgres".to_vec();
        trim_directory(&mut q);
        assert_eq!(q, b"/");
    }

    #[test]
    fn make_relative_path_example() {
        // The doc-comment example from path.c.
        let r = make_relative_path(
            "/usr/local/share/postgresql",
            "/usr/local/bin",
            "/opt/pgsql/bin/postgres",
        );
        assert_eq!(r, "/opt/pgsql/share/postgresql");
    }

    #[test]
    fn make_relative_path_no_match() {
        let r = make_relative_path(
            "/usr/local/share/postgresql",
            "/usr/local/bin",
            "/unrelated/path/postgres",
        );
        assert_eq!(r, "/usr/local/share/postgresql");
    }
}
