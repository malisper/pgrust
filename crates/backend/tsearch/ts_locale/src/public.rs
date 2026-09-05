use core::ffi::{c_char, c_int};
use std::sync::OnceLock;

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::DEFAULT_COLLATION_OID;
use ::types_error::{
    PgError, PgResult, ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
};
use ::wchar::PG_UTF8;

// ts_public.h TSLexeme; a lexize result is PgVec<TsLexeme> (len replaces C's NULL terminator).
pub struct TsLexeme<'mcx> {
    pub nvariant: u16,
    pub flags: u16,
    pub lexeme: PgVec<'mcx, u8>,
}

pub const TSL_ADDPOS: u16 = 0x01;
pub const TSL_PREFIX: u16 = 0x02;
pub const TSL_FILTER: u16 = 0x04;

pub struct DictSubState {
    pub isend: bool,
    pub getnext: bool,
    pub private_state: *mut core::ffi::c_void,
}

pub struct LexDescr {
    pub lexid: i32,
    pub alias: &'static str,
    pub descr: &'static str,
}

pub struct StopList<'mcx> {
    pub stop: PgVec<'mcx, PgVec<'mcx, u8>>,
}

extern "C" {
    fn isalpha(c: c_int) -> c_int;
    fn isalnum(c: c_int) -> c_int;
    fn iswalpha(wc: u32) -> c_int;
    fn iswalnum(wc: u32) -> c_int;
    fn mbstowcs(dest: *mut libc::wchar_t, src: *const c_char, n: usize) -> usize;
}

const WC_BUF_LEN: usize = 3;

fn classify(
    s: &[u8],
    byte_class: unsafe extern "C" fn(c_int) -> c_int,
    wide_class: unsafe extern "C" fn(u32) -> c_int,
) -> bool {
    debug_assert!(!s.is_empty());
    if s.is_empty() {
        return false;
    }
    let clen = ::mbutils::pg_mblen_range(s).unwrap_or(1) as usize;
    if clen == 1 || ::pg_locale::database_ctype_is_c() {
        // SAFETY: pure ctype call on an unsigned-char-range value.
        return unsafe { byte_class(s[0] as c_int) } != 0;
    }
    let mut mb = [0u8; 8];
    mb[..clen].copy_from_slice(&s[..clen]);
    let mut wc: [libc::wchar_t; WC_BUF_LEN] = [0; WC_BUF_LEN];
    // SAFETY: mb is NUL-terminated; at most WC_BUF_LEN wchars written.
    let n = unsafe { mbstowcs(wc.as_mut_ptr(), mb.as_ptr() as *const c_char, WC_BUF_LEN) };
    if n == usize::MAX {
        return false;
    }
    // SAFETY: pure wctype call.
    unsafe { wide_class(wc[0] as u32) != 0 }
}

pub fn t_isalpha(s: &[u8]) -> bool {
    classify(s, isalpha, iswalpha)
}

pub fn t_isalnum(s: &[u8]) -> bool {
    classify(s, isalnum, iswalnum)
}

pub fn t_iseq(s: &[u8], c: u8) -> bool {
    s.first() == Some(&c)
}

pub fn lowerstr<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    ::oracle_compat::casemap::str_tolower(mcx, s, DEFAULT_COLLATION_OID)
}

// Repo-vendored tsearch data staged beside the binary by main_main's
// build.rs (crates/contrib/*/tsearch_data), mirroring the staged extension
// dir: a per-file check, so files the repo doesn't ship keep resolving from
// the system share dir.
fn staged_tsearch_data_dir() -> Option<&'static str> {
    static DIR: OnceLock<Option<String>> = OnceLock::new();
    DIR.get_or_init(|| {
        let exe = std::env::current_exe().ok()?;
        let dir = exe.parent()?.join("share/tsearch_data");
        dir.is_dir().then(|| dir.to_string_lossy().into_owned())
    })
    .as_deref()
}

// DIVERGENCE: PGRUST_PGSHAREDIR env overrides get_share_path(my_exec_path) (tzparser ladder).
fn tsearch_data_dir() -> &'static str {
    static DIR: OnceLock<String> = OnceLock::new();
    DIR.get_or_init(|| {
        if let Ok(share) = std::env::var("PGRUST_PGSHAREDIR") {
            return format!("{share}/tsearch_data");
        }
        if let Some(share) = option_env!("PGRUST_PGSHAREDIR") {
            return format!("{share}/tsearch_data");
        }
        let exec = ::init_small::globals::my_exec_path();
        let len = exec.iter().position(|&b| b == 0).unwrap_or(exec.len());
        let share = ::pg_path::get_share_path(&String::from_utf8_lossy(&exec[..len]));
        format!("{share}/tsearch_data")
    })
}

pub fn get_tsearch_config_filename<'mcx>(
    mcx: Mcx<'mcx>,
    basename: &[u8],
    extension: &str,
) -> PgResult<PgVec<'mcx, u8>> {
    if basename
        .iter()
        .any(|&b| !(b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'))
    {
        return Err(PgError::error(format!(
            "invalid text search configuration file name \"{}\"",
            String::from_utf8_lossy(basename)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into());
    }
    let mut dir = tsearch_data_dir();
    if let Some(staged) = staged_tsearch_data_dir() {
        let candidate = format!(
            "{staged}/{}.{extension}",
            String::from_utf8_lossy(basename)
        );
        if std::path::Path::new(&candidate).is_file() {
            dir = staged;
        }
    }
    let mut out = vec_with_capacity_in(mcx, dir.len() + basename.len() + extension.len() + 2)?;
    out.extend_from_slice(dir.as_bytes());
    out.push(b'/');
    out.extend_from_slice(basename);
    out.push(b'.');
    out.extend_from_slice(extension.as_bytes());
    Ok(out)
}

// ts_locale.c tsearch_readline_state. `tsearch_readline_begin` opens the
// file (an open failure hands back fopen's errno for the caller's
// `could not open ... file "%s": %m` report) and slurps it; `readline` hands
// out one line at a time — lines keep their trailing newline (fgets parity)
// and are recoded from UTF-8 to the database encoding per line, as C does —
// while `context` reproduces tsearch_readline_callback's errcontext for the
// line in flight, which `with_context` attaches to any error raised while
// the file is open (C: the callback sits on error_context_stack from begin
// to end).
pub struct TsearchReadline<'mcx> {
    mcx: Mcx<'mcx>,
    filename: Vec<u8>,
    raw: Vec<u8>,
    pos: usize,
    lineno: usize,
    curline: Option<PgVec<'mcx, u8>>,
}

pub fn tsearch_readline_begin<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &[u8],
) -> Result<TsearchReadline<'mcx>, i32> {
    let path = String::from_utf8_lossy(filename).into_owned();
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => return Err(e.raw_os_error().unwrap_or(0)),
    };
    let mut raw = Vec::new();
    if std::io::Read::read_to_end(&mut file, &mut raw).is_err() {
        // pg_get_line_buf: a read error (ferror) ends the file like EOF
        // and the partial line being collected is dropped — a directory
        // opened for reading, say, is an empty file.
        let keep = raw.iter().rposition(|&b| b == b'\n').map_or(0, |p| p + 1);
        raw.truncate(keep);
    }
    Ok(TsearchReadline {
        mcx,
        filename: filename.to_vec(),
        raw,
        pos: 0,
        lineno: 0,
        curline: None,
    })
}

impl<'mcx> TsearchReadline<'mcx> {
    // tsearch_readline: the next line, freshly copied (C pstrdup's it);
    // Ok(None) at EOF. The recoded line stays in `curline` for `context`.
    pub fn readline(&mut self) -> PgResult<Option<PgVec<'mcx, u8>>> {
        self.lineno += 1;
        self.curline = None;
        if self.pos >= self.raw.len() {
            return Ok(None);
        }
        let rest = &self.raw[self.pos..];
        let end = rest.iter().position(|&b| b == b'\n').map_or(rest.len(), |p| p + 1);
        let line = &rest[..end];
        self.pos += end;
        let recoded = match ::mbutils::pg_any_to_server(self.mcx, line, PG_UTF8)? {
            Some(v) => v,
            None => {
                let mut v = vec_with_capacity_in(self.mcx, line.len())?;
                v.extend_from_slice(line);
                v
            }
        };
        let mut copy = vec_with_capacity_in(self.mcx, recoded.len())?;
        copy.extend_from_slice(&recoded);
        self.curline = Some(recoded);
        Ok(Some(copy))
    }

    // tsearch_readline_callback: no line text for an error raised inside
    // readline itself (an encoding violation, whose bytes C dares not print).
    pub fn context(&self) -> String {
        let filename = String::from_utf8_lossy(&self.filename);
        match &self.curline {
            Some(line) => format!(
                "line {} of configuration file \"{}\": \"{}\"",
                self.lineno,
                filename,
                String::from_utf8_lossy(line)
            ),
            None => format!(
                "line {} of configuration file \"{}\"",
                self.lineno, filename
            ),
        }
    }

    pub fn with_context<T>(&self, r: PgResult<T>) -> PgResult<T> {
        r.map_err(|e| Box::new((*e).add_context(self.context())))
    }
}

// `could not open <kind> file "%s": %m` — spell.c:526/1239/1310/1468,
// ts_utils.c:84, dict_synonym.c:133, dict_thesaurus.c:179: %m is
// strerror(errno) from the failed fopen.
pub fn could_not_open_error(kind: &str, filename: &[u8], errno: i32) -> PgError {
    PgError::error(format!(
        "could not open {kind} file \"{}\": {}",
        String::from_utf8_lossy(filename),
        ::elog::errno::strerror(errno)
    ))
    .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
    .with_saved_errno(errno)
}

pub fn readstoplist<'mcx>(
    mcx: Mcx<'mcx>,
    fname: Option<&[u8]>,
    lower: bool,
) -> PgResult<StopList<'mcx>> {
    let mut stop: PgVec<'mcx, PgVec<'mcx, u8>> = PgVec::new_in(mcx);
    if let Some(fname) = fname.filter(|f| !f.is_empty()) {
        let filename = get_tsearch_config_filename(mcx, fname, "stop")?;
        // ts_utils.c:84 — `could not open stop-word file "%s": %m`.
        let mut rd = match tsearch_readline_begin(mcx, &filename) {
            Ok(rd) => rd,
            Err(errno) => return Err(could_not_open_error("stop-word", &filename, errno).into()),
        };
        // ts_utils.c:87-123: the readline callback is on error_context_stack
        // for the whole loop (wordop included).
        loop {
            let next = rd.readline();
            let Some(line) = rd.with_context(next)? else {
                break;
            };
            let mut end = 0usize;
            while end < line.len() && !byte_isspace(line[end]) {
                end += ::mbutils::pg_mblen(&line[end..]) as usize;
            }
            let end = end.min(line.len());
            if end == 0 {
                continue;
            }
            let word = if lower {
                rd.with_context(lowerstr(mcx, &line[..end]))?
            } else {
                let mut w = vec_with_capacity_in(mcx, end)?;
                w.extend_from_slice(&line[..end]);
                w
            };
            stop.push(word);
        }
    }
    stop.sort_unstable_by(|a, b| a.as_slice().cmp(b.as_slice()));
    Ok(StopList { stop })
}

// C-locale isspace (includes \v, unlike u8::is_ascii_whitespace).
pub fn byte_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

pub fn searchstoplist(s: &StopList<'_>, key: &[u8]) -> bool {
    s.stop
        .binary_search_by(|w| w.as_slice().cmp(key))
        .is_ok()
}
