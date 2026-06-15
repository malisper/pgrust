//! Runtime support layer for the c2rust full-grammar parser (`gram.rs`).
//!
//! `gram.rs` is the c2rust translation of PostgreSQL 18.3's `gram.c` (the bison
//! LR tables + ~4800 action blocks), rewired to construct the *repo's* shared
//! node structs (`backend-nodes-types` + `pgrust-pg-ffi`).  The grammar relies
//! on a set of helper routines that, in C, live in other compilation units
//! (`makefuncs.c`, `list.c`, `value.c`, `palloc`, `elog.c`, `scan.l`/`parser.c`).
//!
//! This module provides those routines as ordinary Rust functions operating on
//! the *same* repo node types the grammar uses, so `base_yyparse` runs as a
//! normal crate function.  In particular it provides:
//!
//!   * a per-parse bump arena backing `palloc`/`palloc0`/`pstrdup`/`psprintf`
//!     (the parser memory context; nodes are never individually freed, matching
//!     PostgreSQL, which resets the whole context after parsing);
//!   * the `make*` node constructors (faithful transcriptions of `makefuncs.c`);
//!   * the `List` helpers (`lappend`/`lcons`/`list_concat`/`list_make*_impl`/…)
//!     implemented over `pgrust_pg_ffi::List`'s public API;
//!   * the value-node makers (`makeInteger`/`makeFloat`/`makeBoolean`/`makeString`);
//!   * C-ABI `errstart`/`errfinish`/… shims that turn `ereport(ERROR, …)` (and a
//!     grammar syntax error via `scanner_yyerror`) into a catchable panic, which
//!     [`raw_parser`] converts back into the C contract of returning `NIL`;
//!   * the `base_yylex` bridge over the repo's tested scanner
//!     (`backend-parser-scan::core_yylex` + `backend-parser-driver::BaseLexer`).

use core::ffi::{c_char, c_int, c_void};

use backend_nodes_types::node_tags::*;
use backend_nodes_types::parsenodes::*;
use backend_nodes_types::parsenodes_ddl::*;
use backend_nodes_types::parsenodes_stmts::*;
use backend_nodes_types::primnodes::*;
use backend_nodes_types::Alias;
use pgrust_pg_ffi::{BitString, Boolean, Float, Integer, List, ListCell, Node, StringNode};

use backend_parser_driver::BaseLexer;
use backend_parser_scan::{tokens, CoreYYSTYPE, Scanner, ScannerSettings, Token, Utf8UnicodeSeam};
use pgrust_pg_ffi::spi::RawParseMode;

use crate::gram::{
    self, base_yy_extra_type, base_yyparse, core_YYSTYPE, parser_init, A_Expr_Kind, BoolExprType,
    CoercionForm, DefElemAction, GroupingSetKind, JsonBehaviorType, JsonEncoding, JsonFormatType,
    JsonValueType, ParseLoc, Size, YYSTYPE,
};

// ===========================================================================
// C-printf shim (replaces csupport.c's variadic psprintf/errmsg family).
// ===========================================================================
//
// gram.rs's actions call `psprintf`/`errmsg`/`errmsg_internal`/`errdetail`/
// `errhint` with C-printf format strings.  In src-idiomatic these were variadic
// C functions in `csupport.c`; here we forbid compiling C, and stable Rust
// cannot *define* a C-variadic, so the grammar's call sites were mechanically
// rewritten to the `pg_psprintf!`/`pg_errmsg!`/`pg_errdetail!`/`pg_errhint!`
// macros below.  The only conversion specifiers the grammar ever passes are
// `%s` (a NUL-terminated `*const c_char`) and `%d` (a C `int`); `cprintf_fmt`
// implements exactly those, byte-for-byte like the C path.

/// A C-printf argument as passed by the (rewritten) grammar call sites: a
/// NUL-terminated C string, or a C `int`.
pub enum CFmtArg {
    /// `%s` — a NUL-terminated `*const c_char` (may be null → "(null)" as glibc).
    Str(*const c_char),
    /// `%d`/`%i` — a C `int`.
    Int(c_int),
}

impl From<*const c_char> for CFmtArg {
    fn from(p: *const c_char) -> Self {
        CFmtArg::Str(p)
    }
}
impl From<*mut c_char> for CFmtArg {
    fn from(p: *mut c_char) -> Self {
        CFmtArg::Str(p as *const c_char)
    }
}
impl From<c_int> for CFmtArg {
    fn from(v: c_int) -> Self {
        CFmtArg::Int(v)
    }
}

/// Render a C-printf `fmt` (NUL-terminated) with `args`, honouring `%s`/`%d`/
/// `%i`/`%%` (the full specifier set the grammar uses).  Returns the rendered
/// bytes (no trailing NUL).
pub unsafe fn cprintf_fmt(fmt: *const c_char, args: &[CFmtArg]) -> Vec<u8> {
    let f = cstr_to_bytes(fmt);
    let mut out: Vec<u8> = Vec::with_capacity(f.len());
    let mut ai = 0usize;
    let mut i = 0usize;
    while i < f.len() {
        let c = f[i];
        if c != b'%' {
            out.push(c);
            i += 1;
            continue;
        }
        i += 1;
        if i >= f.len() {
            out.push(b'%');
            break;
        }
        match f[i] {
            b'%' => out.push(b'%'),
            b's' => {
                if let Some(CFmtArg::Str(p)) = args.get(ai) {
                    if p.is_null() {
                        out.extend_from_slice(b"(null)");
                    } else {
                        out.extend_from_slice(cstr_to_bytes(*p));
                    }
                }
                ai += 1;
            }
            b'd' | b'i' => {
                if let Some(CFmtArg::Int(v)) = args.get(ai) {
                    out.extend_from_slice(v.to_string().as_bytes());
                }
                ai += 1;
            }
            other => {
                // No other specifier occurs in gram.y; emit it literally.
                out.push(b'%');
                out.push(other);
            }
        }
        i += 1;
    }
    out
}

/// `psprintf(fmt, ...)` — format into freshly arena-allocated memory; returns a
/// NUL-terminated `*mut c_char` (replaces csupport.c's `psprintf`).
macro_rules! pg_psprintf {
    ($fmt:expr $(, $arg:expr)* $(,)?) => {{
        let __bytes = unsafe { $crate::support::cprintf_fmt($fmt, &[$($crate::support::CFmtArg::from($arg)),*]) };
        $crate::support::arena_cstr_pub(&__bytes)
    }};
}
pub(crate) use pg_psprintf;

/// `errmsg(fmt, ...)`/`errmsg_internal(fmt, ...)` — record the primary error
/// message for the in-flight `ereport` (replaces csupport.c's `errmsg`).
macro_rules! pg_errmsg {
    ($fmt:expr $(, $arg:expr)* $(,)?) => {{
        let __bytes = unsafe { $crate::support::cprintf_fmt($fmt, &[$($crate::support::CFmtArg::from($arg)),*]) };
        $crate::support::record_errmsg(&__bytes);
        0
    }};
}
pub(crate) use pg_errmsg;

/// `errdetail(fmt, ...)` — detail text is not part of the returned parse tree.
macro_rules! pg_errdetail {
    ($fmt:expr $(, $arg:expr)* $(,)?) => {{ let _ = $fmt; 0 }};
}
pub(crate) use pg_errdetail;

/// `errhint(fmt, ...)` — hint text is not part of the returned parse tree.
macro_rules! pg_errhint {
    ($fmt:expr $(, $arg:expr)* $(,)?) => {{ let _ = $fmt; 0 }};
}
pub(crate) use pg_errhint;

/// Public wrapper over [`arena_cstr`] for the `pg_psprintf!` macro.
pub fn arena_cstr_pub(bytes: &[u8]) -> *mut c_char {
    arena_cstr(bytes)
}

/// Record the primary `ereport` message bytes for the in-flight error (used by
/// the `pg_errmsg!` macro).
pub fn record_errmsg(bytes: &[u8]) {
    let msg = String::from_utf8_lossy(bytes).into_owned();
    CUR_ERRMSG.with(|m| *m.borrow_mut() = msg);
}

// ===========================================================================
// Parser memory context (palloc/palloc0/pstrdup/psprintf).
// ===========================================================================
//
// PostgreSQL parses each query inside a dedicated memory context that is reset
// wholesale afterwards; the grammar never `pfree`s individual nodes (the only
// `pfree` call sites are the yacc value/state stacks).  We mirror that with a
// per-parse bump arena: allocations are leaked into the arena and reclaimed in
// one shot when the arena is dropped at the end of `raw_parser`.

use std::cell::RefCell;

/// A 16-byte, 16-aligned allocation unit; backing chunks are slices of these so
/// every chunk base (and thus every handed-out, `MAXALIGN`-rounded pointer) is
/// 16-byte aligned -- the alignment PostgreSQL's `palloc` guarantees and that
/// every node / `ListCell` requires.
#[repr(C, align(16))]
#[derive(Clone, Copy)]
struct AlignUnit([u8; 16]);

/// A bump-allocation arena: a list of 16-aligned heap chunks whose memory is
/// handed out sequentially and freed all at once on drop.
struct Arena {
    /// (chunk storage, bytes used) pairs.
    chunks: Vec<(Vec<AlignUnit>, usize)>,
}

impl Arena {
    /// Default chunk size in bytes.
    const CHUNK: usize = 64 * 1024;
    const ALIGN: usize = 16;

    fn new() -> Self {
        Arena { chunks: Vec::new() }
    }

    /// Allocate `size` bytes, 16-aligned, valid until the arena is dropped.
    fn alloc(&mut self, size: usize) -> *mut u8 {
        let size = size.max(1);
        // Round the request up to the alignment so the running offset stays
        // 16-aligned for the next allocation.
        let need = (size + Self::ALIGN - 1) & !(Self::ALIGN - 1);

        // Reuse the current chunk if it has room.
        if let Some((buf, used)) = self.chunks.last_mut() {
            let cap_bytes = buf.len() * Self::ALIGN;
            if *used + need <= cap_bytes {
                // SAFETY: `*used` (a multiple of 16) is in-bounds; `buf`'s base
                // is 16-aligned (AlignUnit), so the slot is 16-aligned too.
                let p = unsafe { buf.as_mut_ptr().cast::<u8>().add(*used) };
                *used += need;
                return p;
            }
        }

        // Otherwise start a fresh, zeroed chunk sized to fit at least `need`.
        let bytes = need.max(Self::CHUNK);
        let units = bytes.div_ceil(Self::ALIGN);
        let mut buf = vec![AlignUnit([0u8; 16]); units];
        let p = buf.as_mut_ptr().cast::<u8>();
        self.chunks.push((buf, need));
        p
    }
}

thread_local! {
    /// The active parser arena.  Installed for the duration of a [`raw_parser`]
    /// call; `palloc` and friends allocate from it.
    static ARENA: RefCell<Option<Arena>> = const { RefCell::new(None) };
}

fn arena_alloc(size: usize) -> *mut u8 {
    ARENA.with(|a| {
        a.borrow_mut()
            .as_mut()
            .expect("palloc called outside a raw_parser arena")
            .alloc(size)
    })
}

/// `palloc(size)` -- allocate uninitialized (here: zeroed) memory in the parser
/// context.
pub unsafe fn palloc(size: Size) -> *mut c_void {
    arena_alloc(size as usize).cast()
}

/// `palloc0(size)` -- allocate zeroed memory in the parser context.
pub unsafe fn palloc0(size: Size) -> *mut c_void {
    // Arena memory is always zeroed.
    arena_alloc(size as usize).cast()
}

/// `pfree(p)` -- no-op: the parser context frees everything at once on reset.
pub unsafe fn pfree(_pointer: *mut c_void) {}

/// `pstrdup(s)` -- duplicate a NUL-terminated C string into the parser context.
pub unsafe fn pstrdup(in_0: *const c_char) -> *mut c_char {
    if in_0.is_null() {
        return core::ptr::null_mut();
    }
    let len = libc::strlen(in_0);
    let dst = arena_alloc(len + 1).cast::<c_char>();
    core::ptr::copy_nonoverlapping(in_0, dst, len + 1);
    dst
}

/// Copy a Rust byte slice into a fresh NUL-terminated C string in the arena.
fn arena_cstr(bytes: &[u8]) -> *mut c_char {
    let dst = arena_alloc(bytes.len() + 1).cast::<u8>();
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        *dst.add(bytes.len()) = 0;
    }
    dst.cast()
}

/// Callback from the C variadic shim (`csupport.c`): copy a freshly
/// `vsnprintf`-formatted string into the parser arena.  `psprintf` is variadic
/// and so must be defined in C (Rust's `c_variadic` is unstable); the C side
/// does the real printf formatting and hands the result here.
pub unsafe fn pgrust_gram_arena_strdup(s: *const c_char) -> *mut c_char {
    arena_cstr(cstr_to_bytes(s))
}

/// Borrow the bytes of a NUL-terminated C string (without the NUL).
unsafe fn cstr_to_bytes<'a>(s: *const c_char) -> &'a [u8] {
    if s.is_null() {
        return &[];
    }
    let len = libc::strlen(s);
    core::slice::from_raw_parts(s.cast::<u8>(), len)
}

// ===========================================================================
// Error reporting (ereport / elog -> setjmp/longjmp escape).
// ===========================================================================
//
// gram.y issues `ereport(ERROR, ...)` for semantically invalid input and uses
// `scanner_yyerror` for grammar/lexer syntax errors.  In C these `longjmp` out
// of the parser.  Here the copied `gram.rs` and these support functions are
// plain (non-`extern "C"`) Rust, so a Rust panic unwinds cleanly back out of
// the parser.  The error path records the pending message/SQLSTATE/cursor for
// the caller, then panics with the [`PARSER_ABORT_SENTINEL`] payload; the
// `raw_parser_bytes` driver wraps the parse in `catch_unwind` and converts that
// sentinel back into the C contract of returning `NIL`.  (Any other panic is a
// real bug and is re-raised.)  This replaces the src-idiomatic `csupport.c`
// setjmp/longjmp escape with the workspace's safe-Rust unwinding.

/// Marker payload of the parser's controlled abort panic.
struct ParserAbortSentinel;

/// Escape an in-flight parse the way C's `ereport(ERROR)`/`longjmp` does, via a
/// Rust panic carrying [`ParserAbortSentinel`].  The message/SQLSTATE/cursor
/// have already been recorded in the `CUR_*` thread-locals by the caller.
fn pgrust_gram_error_jump() -> ! {
    std::panic::panic_any(ParserAbortSentinel)
}

thread_local! {
    /// Level passed to the most recent `errstart` (decides whether `errfinish`
    /// must diverge).
    static CUR_ELEVEL: RefCell<c_int> = const { RefCell::new(0) };
    /// Most recent `ereport`/syntax-error message (available to the caller).
    static CUR_ERRMSG: RefCell<String> = const { RefCell::new(String::new()) };
    /// SQLSTATE chars of the most recent error (`errcode()` PGUNSIXBIT-decoded;
    /// `42601` for the yyerror paths; reset to `XX000` by `errstart`).
    static CUR_ERRSTATE: RefCell<[u8; 5]> = const { RefCell::new(*b"XX000") };
    /// 1-based error cursor of the most recent error (`scanner_errposition` /
    /// the yyerror paths); 0 = no position.
    static CUR_ERRPOS: RefCell<c_int> = const { RefCell::new(0) };
    /// Byte location of the token most recently handed to the grammar (the C
    /// `*yyllocp` that `scanner_yyerror` reads for "at or near").
    static LAST_TOK_LOC: RefCell<c_int> = const { RefCell::new(0) };
}

/// `ERROR` elevel (utils/elog.h).  Matches `gram.rs`'s `ERROR` constant.
const ERROR_LEVEL: c_int = 21;

/// The error escape used out of the grammar (longjmp via `csupport.c`).  Kept as
/// a public type for documentation/consumers; the actual escape is the
/// `pgrust_gram_error_jump` longjmp, not a Rust panic.
pub struct ParserAbort {
    pub message: String,
}

/// Read the message recorded by the most recent error (for diagnostics).
pub fn last_error_message() -> String {
    CUR_ERRMSG.with(|m| m.borrow().clone())
}

/// The most recent parse error, for the caller that received NIL from
/// [`raw_parser`]: `(message, sqlstate_chars, 1-based cursor)`.  `None` when no
/// error was recorded since the parse started (NIL then means empty input).
pub fn last_error() -> Option<(String, [u8; 5], c_int)> {
    let msg = CUR_ERRMSG.with(|m| m.borrow().clone());
    if msg.is_empty() {
        return None;
    }
    let state = CUR_ERRSTATE.with(|s| *s.borrow());
    let pos = CUR_ERRPOS.with(|p| *p.borrow());
    Some((msg, state, pos))
}

/// Reset the recorded-error state at the start of a parse (so a NIL result can
/// be distinguished between "error" and "genuinely empty input").
fn clear_last_error() {
    CUR_ERRMSG.with(|m| m.borrow_mut().clear());
    CUR_ERRSTATE.with(|s| *s.borrow_mut() = *b"XX000");
    CUR_ERRPOS.with(|p| *p.borrow_mut() = 0);
    LAST_TOK_LOC.with(|l| *l.borrow_mut() = 0);
}

pub unsafe fn errstart(elevel: c_int, _domain: *const c_char) -> bool {
    CUR_ELEVEL.with(|l| *l.borrow_mut() = elevel);
    CUR_ERRMSG.with(|m| m.borrow_mut().clear());
    // ereport defaults (elog.h): ERRCODE_INTERNAL_ERROR unless errcode() runs.
    CUR_ERRSTATE.with(|s| *s.borrow_mut() = *b"XX000");
    CUR_ERRPOS.with(|p| *p.borrow_mut() = 0);
    // Returning true enters the ereport body so errmsg/errcode/errfinish run.
    true
}

pub unsafe fn errstart_cold(elevel: c_int, domain: *const c_char) -> bool {
    errstart(elevel, domain)
}

pub unsafe fn errfinish(
    _filename: *const c_char,
    _lineno: c_int,
    _funcname: *const c_char,
) {
    let elevel = CUR_ELEVEL.with(|l| *l.borrow());
    if elevel >= ERROR_LEVEL {
        // ereport(ERROR): escape the parser the same way C does.
        pgrust_gram_error_jump();
    }
}

pub unsafe fn errcode(sqlerrcode: c_int) -> c_int {
    // PGUNSIXBIT (elog.h): unpack the MAKE_SQLSTATE 6-bit-per-char encoding.
    let mut chars = [0u8; 5];
    for (i, ch) in chars.iter_mut().enumerate() {
        *ch = (((sqlerrcode >> (6 * i)) & 0x3F) as u8) + b'0';
    }
    CUR_ERRSTATE.with(|s| *s.borrow_mut() = chars);
    0
}

/// Callback from the C variadic shim: record the `vsnprintf`-formatted primary
/// error message for the in-flight `ereport` (used as the [`ParserAbort`]
/// payload).  `errmsg`/`errmsg_internal` are variadic in gram.rs and live in
/// `csupport.c`; they format the message and call this.
pub unsafe fn pgrust_gram_set_errmsg(s: *const c_char) {
    let msg = String::from_utf8_lossy(cstr_to_bytes(s)).into_owned();
    CUR_ERRMSG.with(|m| *m.borrow_mut() = msg);
}

// ===========================================================================
// Scanner bridge: scanner_errposition / scanner_yyerror / base_yylex.
// ===========================================================================
//
// The c2rust grammar calls `base_yylex(lvalp, llocp, yyscanner)`.  `yyscanner`
// is the flex `yyscan_t`: gram.rs dereferences it as `*mut *mut
// base_yy_extra_type` to reach the parse tree.  We honour that contract with a
// [`ShimScanner`] whose first field *is* a `*mut base_yy_extra_type` and whose
// second field is the repo [`BaseLexer`] that actually scans.

/// The opaque object `core_yyscan_t` points at during a parse.
///
/// Field order is load-bearing: `extra` must be first so the grammar's
/// `*(yyscanner as *mut *mut base_yy_extra_type)` resolves to it.
#[repr(C)]
struct ShimScanner<'a> {
    /// `*yyscanner` in flex terms: points at the `base_yy_extra_type` (holding
    /// `parsetree`).
    extra: *mut base_yy_extra_type,
    /// The real scanner/lookahead-filter driving `core_yylex`.
    lexer: *mut BaseLexer<'a>,
}

/// `scanner_errposition(location, yyscanner)` -- compute a 1-based cursor
/// position.  Used only to attach error cursors; for the abort path the exact
/// cursor isn't part of the returned tree, so returning the byte location
/// (clamped to >= 0) is sufficient and matches C for ASCII input.
pub unsafe fn scanner_errposition(
    location: c_int,
    _yyscanner: gram::core_yyscan_t,
) -> c_int {
    let pos = if location < 0 { 0 } else { location + 1 };
    // C: errposition(pos) stores edata->cursorpos; record it for the caller.
    CUR_ERRPOS.with(|p| *p.borrow_mut() = pos);
    pos
}

/// `scanner_yyerror(message, yyscanner)` (scan.l:1163) -- a grammar/lexer
/// syntax error.  In C this `ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR),
/// errmsg("%s at or near \"%s\"", message, yyextra->scanbuf + *yyllocp),
/// lexer_errposition())`; we record the same rendered message + SQLSTATE +
/// cursor and escape via longjmp.
pub unsafe fn scanner_yyerror(message: *const c_char, yyscanner: gram::core_yyscan_t) {
    let msg = String::from_utf8_lossy(cstr_to_bytes(message)).into_owned();
    let lloc = LAST_TOK_LOC.with(|l| *l.borrow());
    record_yyerror(&msg, lloc, yyscanner);
    pgrust_gram_error_jump();
}

/// Render scan.l's `scanner_yyerror` message ("%s at end of input" when the
/// error token is the end-of-buffer, else `%s at or near "<text>"` where the
/// text runs from the offending token's start byte to the scanner's match end
/// -- flex's held NUL in C) and record message/SQLSTATE/cursor.
unsafe fn record_yyerror(message: &str, lloc: c_int, yyscanner: gram::core_yyscan_t) {
    let shim = yyscanner.cast::<ShimScanner>();
    let scanner = (*(*shim).lexer).scanner();
    let buf = scanner.scanbuf();
    let lloc = lloc.max(0) as usize;
    let end = scanner.pos().max(lloc).min(buf.len());
    let full = if lloc >= buf.len() {
        format!("{message} at end of input")
    } else {
        format!(
            "{message} at or near \"{}\"",
            String::from_utf8_lossy(&buf[lloc..end])
        )
    };
    CUR_ERRMSG.with(|m| *m.borrow_mut() = full);
    CUR_ERRSTATE.with(|s| *s.borrow_mut() = *b"42601");
    CUR_ERRPOS.with(|p| {
        *p.borrow_mut() = backend_parser_driver::scanner_errposition(lloc as c_int, buf)
    });
}

/// `base_yylex(lvalp, llocp, yyscanner)` -- the grammar's token source.
///
/// Bridges the repo scanner's [`Token`]/[`CoreYYSTYPE`] into the c2rust
/// grammar's `YYSTYPE` union + `int` location + `int` token-code contract.  The
/// token codes are identical: both the repo `tokens` table and this c2rust
/// `gram` descend from PostgreSQL 18.3's `gram.y`/`gram.h`.
pub unsafe fn base_yylex(
    lvalp: *mut YYSTYPE,
    llocp: *mut c_int,
    yyscanner: gram::core_yyscan_t,
) -> c_int {
    let shim = yyscanner.cast::<ShimScanner>();
    let lexer = &mut *(*shim).lexer;

    let tok = match lexer.base_yylex() {
        Ok(t) => t,
        Err(e) => {
            // Lexer error: escape exactly like scanner_yyerror would.  A
            // yyerror()-path error (scan.l shorthand) gets the C "%s at or
            // near \"%s\"" rendering over the failed match (scanner yylloc ..
            // match end); a direct-ereport lexer error is reported verbatim
            // with its own SQLSTATE, as C does.
            if e.yyerror {
                let lloc = lexer.scanner().yylloc();
                record_yyerror(&e.message, lloc, yyscanner);
            } else {
                CUR_ERRMSG.with(|m| *m.borrow_mut() = e.message);
                CUR_ERRSTATE.with(|s| *s.borrow_mut() = e.sqlstate);
                CUR_ERRPOS.with(|p| *p.borrow_mut() = e.location);
            }
            pgrust_gram_error_jump();
        }
    };

    // Record the token's start byte for scanner_yyerror's "at or near" (the C
    // *yyllocp the grammar hands back on a syntax error).
    LAST_TOK_LOC.with(|l| *l.borrow_mut() = tok.location);

    *llocp = tok.location;
    write_yystype(lvalp, &tok.value);
    tok.token
}

/// Translate a scanned [`CoreYYSTYPE`] into the grammar `YYSTYPE` union, copying
/// any string value into the parser arena (the grammar stores raw `char *`).
unsafe fn write_yystype(lvalp: *mut YYSTYPE, value: &CoreYYSTYPE) {
    // The grammar only reads `core_yystype` (ival / str_0 / keyword) for tokens.
    match value {
        CoreYYSTYPE::Ival(i) => {
            (*lvalp).core_yystype = core_YYSTYPE { ival: *i };
        }
        CoreYYSTYPE::Str(bytes) => {
            (*lvalp).core_yystype = core_YYSTYPE {
                str_0: arena_cstr(bytes),
            };
        }
        CoreYYSTYPE::Keyword(kw) => {
            (*lvalp).core_yystype = core_YYSTYPE {
                keyword: arena_cstr(kw.as_bytes()),
            };
        }
        CoreYYSTYPE::None => {
            (*lvalp).core_yystype = core_YYSTYPE { ival: 0 };
        }
    }
}

// ===========================================================================
// newNode (used by every maker) — gram.rs has its own inline `newNode`; ours
// mirrors it for this module's makers.
// ===========================================================================

#[inline]
unsafe fn new_node(size: usize, tag: gram::NodeTag) -> *mut Node {
    let p = palloc0(size as Size).cast::<Node>();
    (*p).type_ = tag;
    p
}

// ===========================================================================
// List helpers (list.c) over pgrust_pg_ffi::List's public API.
// ===========================================================================

const LIST_HEADER_OVERHEAD_CELLS: usize = 3; // List::header_overhead_cells() == 3

fn pg_nextpower2_32(mut v: u32) -> u32 {
    if v <= 1 {
        return 1;
    }
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v + 1
}

/// `new_list(type, min_size)` (list.c) -- allocate a list with `min_size`
/// cells already counted, sized to the next power of two.
unsafe fn new_list(type_0: gram::NodeTag, min_size: c_int) -> *mut List {
    let want = (min_size as usize) + LIST_HEADER_OVERHEAD_CELLS;
    let max_size = (pg_nextpower2_32((8usize.max(want)) as u32) as c_int)
        - LIST_HEADER_OVERHEAD_CELLS as c_int;
    let bytes = List::header_size() + (max_size as usize) * core::mem::size_of::<ListCell>();
    let raw = palloc(bytes as Size).cast::<List>();
    List::initialize(raw, type_0, min_size, max_size);
    raw
}

/// `enlarge_list(list, min_size)` -- grow a list's cell storage in place.
unsafe fn enlarge_list(list: *mut List, min_size: c_int) {
    let new_max = pg_nextpower2_32(16u32.max(min_size as u32)) as c_int;
    let old = (*list).len();
    let new_elems =
        palloc((new_max as usize * core::mem::size_of::<ListCell>()) as Size).cast::<ListCell>();
    core::ptr::copy_nonoverlapping((*list).elements_ptr(), new_elems, old as usize);
    (*list).set_elements_ptr(new_elems);
    (*list).set_max_length(new_max);
}

unsafe fn new_tail_cell(list: *mut List) {
    if (*list).len() >= (*list).max_length() {
        enlarge_list(list, (*list).len() + 1);
    }
    (*list).set_len((*list).len() + 1);
}

unsafe fn new_head_cell(list: *mut List) {
    if (*list).len() >= (*list).max_length() {
        enlarge_list(list, (*list).len() + 1);
    }
    let elems = (*list).elements_ptr();
    let len = (*list).len() as usize;
    core::ptr::copy(elems, elems.add(1), len);
    (*list).set_len((*list).len() + 1);
}

unsafe fn last_cell(list: *mut List) -> *mut ListCell {
    (*list).elements_ptr().add(((*list).len() - 1) as usize)
}

pub unsafe fn list_make1_impl(t: gram::NodeTag, datum1: ListCell) -> *mut List {
    let list = new_list(t, 1);
    *(*list).elements_ptr().add(0) = datum1;
    list
}

pub unsafe fn list_make2_impl(
    t: gram::NodeTag,
    datum1: ListCell,
    datum2: ListCell,
) -> *mut List {
    let list = new_list(t, 2);
    *(*list).elements_ptr().add(0) = datum1;
    *(*list).elements_ptr().add(1) = datum2;
    list
}

pub unsafe fn list_make3_impl(
    t: gram::NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
) -> *mut List {
    let list = new_list(t, 3);
    *(*list).elements_ptr().add(0) = datum1;
    *(*list).elements_ptr().add(1) = datum2;
    *(*list).elements_ptr().add(2) = datum3;
    list
}

pub unsafe fn list_make4_impl(
    t: gram::NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
    datum4: ListCell,
) -> *mut List {
    let list = new_list(t, 4);
    *(*list).elements_ptr().add(0) = datum1;
    *(*list).elements_ptr().add(1) = datum2;
    *(*list).elements_ptr().add(2) = datum3;
    *(*list).elements_ptr().add(3) = datum4;
    list
}

pub unsafe fn lappend(mut list: *mut List, datum: *mut c_void) -> *mut List {
    if list.is_null() {
        list = new_list(T_List, 1);
    } else {
        new_tail_cell(list);
    }
    (*last_cell(list)).ptr_value = datum;
    list
}

pub unsafe fn lcons(datum: *mut c_void, mut list: *mut List) -> *mut List {
    if list.is_null() {
        list = new_list(T_List, 1);
    } else {
        new_head_cell(list);
    }
    *(*list).elements_ptr().add(0) = ListCell { ptr_value: datum };
    list
}

unsafe fn list_copy(oldlist: *const List) -> *mut List {
    if oldlist.is_null() {
        return core::ptr::null_mut();
    }
    let len = (*oldlist).len();
    let newlist = new_list((*oldlist).list_type(), len);
    core::ptr::copy_nonoverlapping(
        (*oldlist).elements_ptr(),
        (*newlist).elements_ptr(),
        len as usize,
    );
    newlist
}

pub unsafe fn list_concat(list1: *mut List, list2: *const List) -> *mut List {
    if list1.is_null() {
        return list_copy(list2);
    }
    if list2.is_null() {
        return list1;
    }
    let new_len = (*list1).len() + (*list2).len();
    if new_len > (*list1).max_length() {
        enlarge_list(list1, new_len);
    }
    core::ptr::copy_nonoverlapping(
        (*list2).elements_ptr(),
        (*list1).elements_ptr().add((*list1).len() as usize),
        (*list2).len() as usize,
    );
    (*list1).set_len(new_len);
    list1
}

pub unsafe fn list_truncate(list: *mut List, new_size: c_int) -> *mut List {
    if new_size <= 0 {
        return core::ptr::null_mut();
    }
    if !list.is_null() && new_size < (*list).len() {
        (*list).set_len(new_size);
    }
    list
}

pub unsafe fn list_delete_nth_cell(list: *mut List, n: c_int) -> *mut List {
    let len = (*list).len();
    if len == 1 {
        return core::ptr::null_mut();
    }
    let elems = (*list).elements_ptr();
    // memmove(elems[n], elems[n+1], (len-1-n) cells)
    core::ptr::copy(
        elems.add((n + 1) as usize),
        elems.add(n as usize),
        (len - 1 - n) as usize,
    );
    (*list).set_len(len - 1);
    list
}

pub unsafe fn list_copy_tail(oldlist: *const List, mut nskip: c_int) -> *mut List {
    if nskip < 0 {
        nskip = 0;
    }
    if oldlist.is_null() || nskip >= (*oldlist).len() {
        return core::ptr::null_mut();
    }
    let len = (*oldlist).len() - nskip;
    let newlist = new_list((*oldlist).list_type(), len);
    core::ptr::copy_nonoverlapping(
        (*oldlist).elements_ptr().add(nskip as usize),
        (*newlist).elements_ptr(),
        len as usize,
    );
    newlist
}

#[inline]
unsafe fn list_length(l: *const List) -> c_int {
    if l.is_null() {
        0
    } else {
        (*l).len()
    }
}

// ===========================================================================
// Value-node makers (value.c).
// ===========================================================================

pub unsafe fn makeInteger(i: c_int) -> *mut Integer {
    let v = new_node(core::mem::size_of::<Integer>(), T_Integer).cast::<Integer>();
    (*v).ival = i;
    v
}

pub unsafe fn makeFloat(numeric_str: *mut c_char) -> *mut Float {
    let v = new_node(core::mem::size_of::<Float>(), T_Float).cast::<Float>();
    (*v).fval = numeric_str;
    v
}

pub unsafe fn makeBoolean(val: bool) -> *mut Boolean {
    let v = new_node(core::mem::size_of::<Boolean>(), T_Boolean).cast::<Boolean>();
    (*v).boolval = val;
    v
}

pub unsafe fn makeString(str: *mut c_char) -> *mut StringNode {
    let v = new_node(core::mem::size_of::<StringNode>(), T_String).cast::<StringNode>();
    (*v).sval = str;
    v
}

#[allow(dead_code)]
pub unsafe fn makeBitString(str: *mut c_char) -> *mut BitString {
    let v = new_node(core::mem::size_of::<BitString>(), T_BitString).cast::<BitString>();
    (*v).bsval = str;
    v
}

// ===========================================================================
// Node makers (makefuncs.c), faithful transcriptions over repo node types.
// ===========================================================================

pub unsafe fn makeA_Expr(
    kind: A_Expr_Kind,
    name: *mut List,
    lexpr: *mut Node,
    rexpr: *mut Node,
    location: c_int,
) -> *mut A_Expr {
    let a = new_node(core::mem::size_of::<A_Expr>(), T_A_Expr).cast::<A_Expr>();
    (*a).kind = kind;
    (*a).name = name;
    (*a).lexpr = lexpr;
    (*a).rexpr = rexpr;
    (*a).location = location as ParseLoc;
    a
}

pub unsafe fn makeSimpleA_Expr(
    kind: A_Expr_Kind,
    name: *mut c_char,
    lexpr: *mut Node,
    rexpr: *mut Node,
    location: c_int,
) -> *mut A_Expr {
    let a = new_node(core::mem::size_of::<A_Expr>(), T_A_Expr).cast::<A_Expr>();
    (*a).kind = kind;
    (*a).name = list_make1_impl(
        T_List,
        ListCell {
            ptr_value: makeString(name).cast(),
        },
    );
    (*a).lexpr = lexpr;
    (*a).rexpr = rexpr;
    (*a).location = location as ParseLoc;
    a
}

pub unsafe fn makeBoolExpr(
    boolop: BoolExprType,
    args: *mut List,
    location: c_int,
) -> *mut Expr {
    let b = new_node(core::mem::size_of::<BoolExpr>(), T_BoolExpr).cast::<BoolExpr>();
    (*b).boolop = boolop;
    (*b).args = args;
    (*b).location = location as ParseLoc;
    b.cast()
}

pub unsafe fn makeAlias(aliasname: *const c_char, colnames: *mut List) -> *mut Alias {
    let a = new_node(core::mem::size_of::<Alias>(), T_Alias).cast::<Alias>();
    (*a).aliasname = pstrdup(aliasname);
    (*a).colnames = colnames;
    a
}

pub unsafe fn makeRangeVar(
    schemaname: *mut c_char,
    relname: *mut c_char,
    location: c_int,
) -> *mut RangeVar {
    let r = new_node(core::mem::size_of::<RangeVar>(), T_RangeVar).cast::<RangeVar>();
    (*r).catalogname = core::ptr::null_mut();
    (*r).schemaname = schemaname;
    (*r).relname = relname;
    (*r).inh = true;
    (*r).relpersistence = gram::RELPERSISTENCE_PERMANENT as c_char;
    (*r).alias = core::ptr::null_mut();
    (*r).location = location as ParseLoc;
    r
}

pub unsafe fn makeTypeName(typnam: *mut c_char) -> *mut TypeName {
    makeTypeNameFromNameList(list_make1_impl(
        T_List,
        ListCell {
            ptr_value: makeString(typnam).cast(),
        },
    ))
}

pub unsafe fn makeTypeNameFromNameList(names: *mut List) -> *mut TypeName {
    let n = new_node(core::mem::size_of::<TypeName>(), T_TypeName).cast::<TypeName>();
    (*n).names = names;
    (*n).typmods = core::ptr::null_mut();
    (*n).typemod = -1;
    (*n).location = -1;
    n
}

pub unsafe fn makeFuncCall(
    name: *mut List,
    args: *mut List,
    funcformat: CoercionForm,
    location: c_int,
) -> *mut FuncCall {
    let n = new_node(core::mem::size_of::<FuncCall>(), T_FuncCall).cast::<FuncCall>();
    (*n).funcname = name;
    (*n).args = args;
    (*n).agg_order = core::ptr::null_mut();
    (*n).agg_filter = core::ptr::null_mut();
    (*n).over = core::ptr::null_mut();
    (*n).agg_within_group = false;
    (*n).agg_star = false;
    (*n).agg_distinct = false;
    (*n).func_variadic = false;
    (*n).funcformat = funcformat;
    (*n).location = location as ParseLoc;
    n
}

pub unsafe fn makeStringConst(str: *mut c_char, location: c_int) -> *mut Node {
    let n = new_node(core::mem::size_of::<A_Const>(), T_A_Const).cast::<A_Const>();
    (*n).val.sval.type_ = T_String;
    (*n).val.sval.sval = str;
    (*n).location = location as ParseLoc;
    n.cast()
}

pub unsafe fn makeDefElem(
    name: *mut c_char,
    arg: *mut Node,
    location: c_int,
) -> *mut DefElem {
    let res = new_node(core::mem::size_of::<DefElem>(), T_DefElem).cast::<DefElem>();
    (*res).defnamespace = core::ptr::null_mut();
    (*res).defname = name;
    (*res).arg = arg;
    (*res).defaction = gram::DEFELEM_UNSPEC;
    (*res).location = location as ParseLoc;
    res
}

pub unsafe fn makeDefElemExtended(
    name_space: *mut c_char,
    name: *mut c_char,
    arg: *mut Node,
    defaction: DefElemAction,
    location: c_int,
) -> *mut DefElem {
    let res = new_node(core::mem::size_of::<DefElem>(), T_DefElem).cast::<DefElem>();
    (*res).defnamespace = name_space;
    (*res).defname = name;
    (*res).arg = arg;
    (*res).defaction = defaction;
    (*res).location = location as ParseLoc;
    res
}

pub unsafe fn makeGroupingSet(
    kind: GroupingSetKind,
    content: *mut List,
    location: c_int,
) -> *mut GroupingSet {
    let n = new_node(core::mem::size_of::<GroupingSet>(), T_GroupingSet).cast::<GroupingSet>();
    (*n).kind = kind;
    (*n).content = content;
    (*n).location = location as ParseLoc;
    n
}

pub unsafe fn makeVacuumRelation(
    relation: *mut RangeVar,
    oid: gram::Oid,
    va_cols: *mut List,
) -> *mut VacuumRelation {
    let v =
        new_node(core::mem::size_of::<VacuumRelation>(), T_VacuumRelation).cast::<VacuumRelation>();
    (*v).relation = relation;
    (*v).oid = oid;
    (*v).va_cols = va_cols;
    v
}

pub unsafe fn makeJsonFormat(
    type_: JsonFormatType,
    encoding: JsonEncoding,
    location: c_int,
) -> *mut JsonFormat {
    let jf = new_node(core::mem::size_of::<JsonFormat>(), T_JsonFormat).cast::<JsonFormat>();
    (*jf).format_type = type_;
    (*jf).encoding = encoding;
    (*jf).location = location as ParseLoc;
    jf
}

pub unsafe fn makeJsonValueExpr(
    raw_expr: *mut Expr,
    formatted_expr: *mut Expr,
    format: *mut JsonFormat,
) -> *mut JsonValueExpr {
    let jve =
        new_node(core::mem::size_of::<JsonValueExpr>(), T_JsonValueExpr).cast::<JsonValueExpr>();
    (*jve).raw_expr = raw_expr;
    (*jve).formatted_expr = formatted_expr;
    (*jve).format = format;
    jve
}

pub unsafe fn makeJsonBehavior(
    btype: JsonBehaviorType,
    expr: *mut Node,
    location: c_int,
) -> *mut JsonBehavior {
    let behavior =
        new_node(core::mem::size_of::<JsonBehavior>(), T_JsonBehavior).cast::<JsonBehavior>();
    (*behavior).btype = btype;
    (*behavior).expr = expr;
    (*behavior).location = location as ParseLoc;
    behavior
}

pub unsafe fn makeJsonKeyValue(key: *mut Node, value: *mut Node) -> *mut Node {
    let n = new_node(core::mem::size_of::<JsonKeyValue>(), T_JsonKeyValue).cast::<JsonKeyValue>();
    (*n).key = key.cast();
    (*n).value = value.cast();
    n.cast()
}

pub unsafe fn makeJsonIsPredicate(
    expr: *mut Node,
    format: *mut JsonFormat,
    item_type: JsonValueType,
    unique_keys: bool,
    location: c_int,
) -> *mut Node {
    let n = new_node(core::mem::size_of::<JsonIsPredicate>(), T_JsonIsPredicate)
        .cast::<JsonIsPredicate>();
    (*n).expr = expr;
    (*n).format = format;
    (*n).item_type = item_type;
    (*n).unique_keys = unique_keys;
    (*n).location = location as ParseLoc;
    n.cast()
}

pub unsafe fn makeJsonTablePathSpec(
    string: *mut c_char,
    name: *mut c_char,
    string_location: c_int,
    name_location: c_int,
) -> *mut JsonTablePathSpec {
    let pathspec = new_node(
        core::mem::size_of::<JsonTablePathSpec>(),
        T_JsonTablePathSpec,
    )
    .cast::<JsonTablePathSpec>();
    (*pathspec).string = makeStringConst(string, string_location);
    if !name.is_null() {
        (*pathspec).name = pstrdup(name);
    }
    (*pathspec).name_location = name_location as ParseLoc;
    (*pathspec).location = string_location as ParseLoc;
    pathspec
}

// ===========================================================================
// Misc grammar helpers.
// ===========================================================================

/// `strcmp` — the grammar compares NUL-terminated C strings; route to libc.
pub unsafe fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int {
    libc::strcmp(s1, s2)
}

/// `pg_strcasecmp` (port/pgstrcasecmp.c) -- ASCII case-insensitive compare.
pub unsafe fn pg_strcasecmp(mut s1: *const c_char, mut s2: *const c_char) -> c_int {
    loop {
        let mut ch1 = *s1 as u8;
        let mut ch2 = *s2 as u8;
        if ch1 != ch2 {
            if ch1.is_ascii_uppercase() {
                ch1 += b'a' - b'A';
            }
            if ch2.is_ascii_uppercase() {
                ch2 += b'a' - b'A';
            }
            if ch1 != ch2 {
                return ch1 as c_int - ch2 as c_int;
            }
        }
        if ch1 == 0 {
            break;
        }
        s1 = s1.add(1);
        s2 = s2.add(1);
    }
    0
}

/// `NameListToString(names)` (varlena.c) -- render a dotted name list, quoting
/// only where C would.  Used by gram.y solely in error messages.
pub unsafe fn NameListToString(names: *const List) -> *mut c_char {
    let mut out: Vec<u8> = Vec::new();
    if !names.is_null() {
        let len = (*names).len();
        let elems = (*names).elements_ptr();
        for i in 0..len {
            if i > 0 {
                out.push(b'.');
            }
            let node = (*elems.add(i as usize)).ptr_value.cast::<Node>();
            if node.is_null() {
                continue;
            }
            match (*node).type_ {
                T_String => {
                    let s = (*node.cast::<StringNode>()).sval;
                    out.extend_from_slice(cstr_to_bytes(s));
                }
                T_A_Star => out.push(b'*'),
                _ => {}
            }
        }
    }
    arena_cstr(&out)
}

/// `defGetInt32(def)` (define.c) -- read an integer DefElem argument.  gram.y
/// uses it for PARTITION `MODULUS`/`REMAINDER`, whose args are Integer values.
pub unsafe fn defGetInt32(def: *mut DefElem) -> gram::int32 {
    let arg = (*def).arg;
    if arg.is_null() {
        return 0;
    }
    if (*arg).type_ == T_Integer {
        (*arg.cast::<Integer>()).ival
    } else {
        0
    }
}

/// `exprLocation(expr)` (nodeFuncs.c) -- the parse location of a node.  gram.y
/// calls this only on the duplicate-clause error paths, on a `List` of sort
/// items or a `WithClause`; we cover those (and `SortBy`) faithfully and return
/// -1 for any other tag (matching C's "unknown -> -1" default).
pub unsafe fn exprLocation(expr: *const Node) -> c_int {
    if expr.is_null() {
        return -1;
    }
    match (*expr).type_ {
        T_List => {
            // C: min over all members' exprLocation.
            let list = expr.cast::<List>();
            let mut loc = -1i32;
            let len = (*list).len();
            let elems = (*list).elements_ptr();
            for i in 0..len {
                let child = (*elems.add(i as usize)).ptr_value.cast::<Node>();
                let cl = exprLocation(child);
                if cl >= 0 && (loc < 0 || cl < loc) {
                    loc = cl;
                }
            }
            loc
        }
        T_SortBy => (*expr.cast::<SortBy>()).location,
        T_WithClause => (*expr.cast::<WithClause>()).location,
        _ => -1,
    }
}

/// `copyObjectImpl(from)` (copyfuncs.c) -- gram.y copies an already-built
/// `TypeName` here (function-parameter argtype duplication).  A shallow copy of
/// the `TypeName` node preserves the parse tree's referential structure for the
/// grammar's purposes.
pub unsafe fn copyObjectImpl(from: *const c_void) -> *mut c_void {
    if from.is_null() {
        return core::ptr::null_mut();
    }
    let node = from.cast::<Node>();
    match (*node).type_ {
        T_TypeName => {
            let dst = new_node(core::mem::size_of::<TypeName>(), T_TypeName).cast::<TypeName>();
            *dst = *from.cast::<TypeName>();
            (*dst).type_ = T_TypeName;
            dst.cast()
        }
        _ => from.cast_mut(),
    }
}

/// `equal(a, b)` (equalfuncs.c) -- gram.y compares two `TypeName`s (ordered-set
/// aggregate direct/aggregated argtype check).  Compares the dotted type names.
pub unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    if a == b {
        return true;
    }
    if a.is_null() || b.is_null() {
        return false;
    }
    let na = a.cast::<Node>();
    let nb = b.cast::<Node>();
    if (*na).type_ != (*nb).type_ {
        return false;
    }
    if (*na).type_ == T_TypeName {
        let sa = NameListToString((*a.cast::<TypeName>()).names);
        let sb = NameListToString((*b.cast::<TypeName>()).names);
        return libc::strcmp(sa, sb) == 0;
    }
    false
}

// ===========================================================================
// Public entry: raw_parser.
// ===========================================================================

thread_local! {
    /// The query bytes for the in-flight `raw_parser` call (kept alive across
    /// the C `setjmp` protected call so the scanner can borrow them).  Held as a
    /// boxed slice so we can read a raw `(ptr, len)` *without* keeping a RefCell
    /// borrow guard across `base_yyparse` (a longjmp would skip the guard's
    /// Drop and poison the cell).
    static CUR_SQL: RefCell<Option<Box<[u8]>>> = const { RefCell::new(None) };
    /// The `RawParseMode` for the in-flight call.
    static CUR_MODE: RefCell<RawParseMode> = const { RefCell::new(0) };
}

/// `raw_parser(str, mode)` (parser.c:41) -- lexical + grammatical analysis of a
/// query string, returning the `List *` of raw (un-analyzed) `RawStmt` parse
/// trees as repo nodes.  Returns `NIL` on a syntax / semantic parse error,
/// matching the C contract.
///
/// This is the entry the parser seam (`BootPgRuntime.raw_parser`) calls.  The
/// returned nodes live in the per-call parser arena, which is *leaked* for the
/// lifetime of the process the same way PostgreSQL keeps them in the parse
/// memory context until the caller is done; callers must treat the result as
/// living in that context.
pub fn raw_parser(sql: &str, mode: RawParseMode) -> *mut List {
    raw_parser_bytes(sql.as_bytes(), mode)
}

/// As [`raw_parser`], taking raw query bytes.
pub fn raw_parser_bytes(sql: &[u8], mode: RawParseMode) -> *mut List {
    // Stage the query for `run_inner` and install a fresh parser arena.
    CUR_SQL.with(|s| *s.borrow_mut() = Some(sql.to_vec().into_boxed_slice()));
    CUR_MODE.with(|m| *m.borrow_mut() = mode);
    // Reset the recorded-error state so a NIL result can be told apart:
    // last_error() == None then means genuinely empty input, not an error.
    clear_last_error();
    install_arena();

    // Drive the parse under a Rust unwind guard: an ereport(ERROR)/syntax error
    // panics with `ParserAbortSentinel`, which we catch here and turn into NULL
    // (NIL), the C `raw_parser` contract.  Any non-sentinel panic is a genuine
    // bug and is re-raised.
    let tree = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        run_inner()
    })) {
        Ok(p) => p.cast::<List>(),
        Err(payload) => {
            if payload.downcast_ref::<ParserAbortSentinel>().is_some() {
                core::ptr::null_mut::<List>()
            } else {
                // Detach the arena before re-raising so process teardown of a
                // poisoned thread-local does not double-panic.
                leak_arena();
                CUR_SQL.with(|s| *s.borrow_mut() = None);
                std::panic::resume_unwind(payload);
            }
        }
    };

    // Detach the arena (kept alive in LEAKED_ARENAS so result pointers survive).
    leak_arena();
    CUR_SQL.with(|s| *s.borrow_mut() = None);

    tree
}

/// Drive scanner + grammar for the staged query under the unwind guard.
unsafe fn run_inner() -> *mut c_void {
    let mode = CUR_MODE.with(|m| *m.borrow());

    // Read a raw (ptr, len) view of the staged query *without* keeping a RefCell
    // borrow alive across base_yyparse: a longjmp out of the parser would skip
    // the guard's Drop and poison the cell.  The boxed slice stays alive in
    // CUR_SQL until raw_parser_bytes clears it after the protected call returns.
    let (ptr, len) = CUR_SQL.with(|s| match &*s.borrow() {
        Some(b) => (b.as_ptr(), b.len()),
        None => (core::ptr::null(), 0),
    });
    let sql: &[u8] = core::slice::from_raw_parts(ptr, len);

    // Build the repo scanner + base_yylex filter, seeding the mode lookahead.
    // ScannerSettings::live(): the live `standard_conforming_strings` /
    // `escape_string_warning` / `backslash_quote` GUCs (scan.l:68-70) — the
    // compiled-in defaults silently accepted U& strings that `SET
    // standard_conforming_strings = off` must reject (scan.l xusstart).
    let scanner = Scanner::new(sql, ScannerSettings::live());
    let seed = mode_token(mode).map(|tok| Token {
        token: tok,
        value: CoreYYSTYPE::None,
        location: 0,
    });
    let mut lexer = BaseLexer::new(scanner, seed, &Utf8UnicodeSeam);

    // The flex-shaped yyscanner: first field is the *base_yy_extra_type, second
    // is the live BaseLexer the base_yylex shim reads from.
    let mut extra: base_yy_extra_type = core::mem::zeroed();
    let mut shim = ShimScanner {
        extra: &mut extra,
        lexer: &mut lexer,
    };
    let yyscanner = (&mut shim as *mut ShimScanner).cast::<c_void>();

    parser_init(&mut extra);
    let yyresult = base_yyparse(yyscanner);
    if yyresult != 0 {
        return core::ptr::null_mut::<c_void>();
    }
    extra.parsetree.cast()
}

/// `mode_token[]` (parser.c:58) -- initial lookahead token for non-default
/// parse modes.
fn mode_token(mode: RawParseMode) -> Option<i32> {
    use pgrust_pg_ffi::spi::*;
    match mode {
        m if m == RAW_PARSE_DEFAULT => None,
        m if m == RAW_PARSE_TYPE_NAME => Some(tokens::MODE_TYPE_NAME),
        m if m == RAW_PARSE_PLPGSQL_EXPR => Some(tokens::MODE_PLPGSQL_EXPR),
        m if m == RAW_PARSE_PLPGSQL_ASSIGN1 => Some(tokens::MODE_PLPGSQL_ASSIGN1),
        m if m == RAW_PARSE_PLPGSQL_ASSIGN2 => Some(tokens::MODE_PLPGSQL_ASSIGN2),
        m if m == RAW_PARSE_PLPGSQL_ASSIGN3 => Some(tokens::MODE_PLPGSQL_ASSIGN3),
        _ => None,
    }
}

thread_local! {
    /// Arenas detached after a parse but whose nodes are still referenced by the
    /// returned tree.  Kept alive for the process; PostgreSQL likewise frees the
    /// parse context only when the caller is finished with the tree.
    static LEAKED_ARENAS: RefCell<Vec<Arena>> = const { RefCell::new(Vec::new()) };
}

fn install_arena() {
    ARENA.with(|a| *a.borrow_mut() = Some(Arena::new()));
}

fn leak_arena() {
    ARENA.with(|a| {
        if let Some(arena) = a.borrow_mut().take() {
            LEAKED_ARENAS.with(|l| l.borrow_mut().push(arena));
        }
    });
}
