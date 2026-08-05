//! jsonpathexec_diff: differential fuzz driver — shipped Rust
//! `adt_jsonpath_exec` (path-eval core + fc_ wrappers) vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/jsonpath/*: verbatim jsonpath_exec.c + jsonb_util.c + regexec.c on
//! top of the jsonpath_diff family's verbatim oracle).
//! Crate under test: crates/backend/utils/adt/jsonpath_exec.
//!
//! INPUT-STRATEGY DECISION (recorded per the lane charter): the driver
//! parses the JSON document and the vars document ONCE with the shipped
//! Rust `adt_jsonb` crate and feeds the identical jsonb IMAGE BYTES to both
//! engines (the C JsonbContainer layout is byte-identical by design; the
//! Rust reader and the C reader interpret one image). Chosen over vendoring
//! C-side jsonb_in/json.c/jsonapi.c because (a) it removes an entire
//! parser call graph from the oracle, and (b) it keeps THIS differential
//! focused on path-eval: doc-parse/serialize parity is a cross-crate
//! adt/jsonb plane owned by lane p1-lanev, and any divergence there would
//! be triaged to that lane anyway (recorded, not fixed here). The image
//! invariant is exercised on every iteration: both engines walk the same
//! bytes with independent container readers, so a layout misinterpretation
//! on either side surfaces as a result-plane divergence.
//!
//! Input layout: [sel][s1 u16le][s2 u16le][rest]; rest is split into
//! path-text / doc-json / vars-json at offsets derived from s1/s2.
//! sel bits: arm = sel & 7 mapped % 7 (0 exists, 1 match, 2 query_array,
//! 3 query_first, 4 exists_opr (@?), 5 match_opr (@@), 6 query_items =
//! the pure row-collection core of the SRF jsonb_path_query);
//! silent = sel bit 3, tz = sel bit 4 (routes to the _tz wrappers;
//! opr arms ignore both — @? / @@ are silent=true, tz=false by catalog),
//! vars-present = sel bit 5 (absent models the SQL-level `vars => '{}'`
//! default with an empty-object image, exactly what the server passes).
//!
//! Comparison planes: result verdict (ok-true / ok-false / ok-NULL /
//! hard-error), errcode/sqlstate, result jsonb image bytes
//! (query_array / query_first byte-exact; query_items per-row byte-exact),
//! plus the FC-WRAPPER PLANE: each arm re-drives the (already core-vs-C
//! checked) input through the crate's fc_* wrapper over a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper == core, so the
//! builtins.rs lines execute every iteration with an in-harness oracle.
//!
//! PINNED ENVIRONMENT: same as jsonpath_diff (UTF-8 server encoding on
//! both sides; C-ctype default collation on both sides — like_regex
//! compiles and EXECUTES under PG_REGEX_STRATEGY_C on both engines).
//!
//! CARVE-OUTS (documented per the skill's rules):
//!   - DATETIME METHOD FAMILY (ruled in the claim): .datetime()/.date()/
//!     .time()/.time_tz()/.timestamp()/.timestamp_tz() and the
//!     jbvDatetime comparison paths read session-timezone state
//!     (lib.rs execute_datetime_method / compare_datetime /
//!     session_tz_offset / encode_datetime). Carved AT THE DRIVER LEVEL:
//!     `path_has_datetime_item` walks the PARSED item tree (not the text,
//!     so a key named "datetime" stays in-domain) and skips such paths on
//!     BOTH engines; the C oracle's whole datetime call graph is
//!     loud-abort sentinel stubs that must never fire
//!     (pg_jsonpath_exec_env.c). Carve hit-rate counters are exported
//!     (CARVE_HITS / EXEC_TOTAL) and recorded in the README from smoke.
//!   - SRF/MultiFuncCall plumbing of jsonb_path_query (fc_jsonb_path_query
//!     / _tz): out of scope; the pure row-collection core
//!     (jsonb_path_query_core) IS covered by arm 6.
//!   - json_table.rs: out of scope entirely (ruled in the claim).
//!   - stack-depth exhaustion (54001): input caps bound recursion below
//!     both engines' real guards, same as jsonpath_diff.
//!   - message/detail text: out of scope (sqlstate is the error-identity
//!     plane).
//!   - invalid UTF-8 / interior NUL in path, doc, or vars text: the
//!     server input pipeline validates client bytes before any input
//!     function runs (see jsonpath_diff divergence 1); all three texts
//!     are required to be NUL-free valid UTF-8.
//!   - path-parse plane: the path must parse on BOTH sides (verdict-equal,
//!     image-equal) or the input is skipped — parse-plane divergences are
//!     jsonpath_diff's covered plane, not this target's. On agreement the
//!     ONE agreed image is fed to both engines.

use core::ffi::c_int;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

use adt_jsonpath::path::ItemType;
use datum::{Datum, NullableDatum};
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, LocalFcinfo};

use adt_jsonpath_exec::JsonPathVars;

extern "C" {
    // path parse (jsonpath_diff family oracle)
    fn pg_diff_jsonpath_in(
        s: *const u8,
        len: usize,
        soft: c_int,
        image_out: *mut *const u8,
        image_len: *mut usize,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonpath_last_msg() -> *const core::ffi::c_char;

    // exec oracle entries (pg_jsonpath_exec_env.c)
    fn pg_diff_jsonb_path_exists(
        doc: *const u8,
        doc_len: usize,
        path: *const u8,
        path_len: usize,
        vars: *const u8,
        vars_len: usize,
        silent: c_int,
        tz: c_int,
        opr: c_int,
        res: *mut c_int,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonb_path_match(
        doc: *const u8,
        doc_len: usize,
        path: *const u8,
        path_len: usize,
        vars: *const u8,
        vars_len: usize,
        silent: c_int,
        tz: c_int,
        opr: c_int,
        res: *mut c_int,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonb_path_query_array(
        doc: *const u8,
        doc_len: usize,
        path: *const u8,
        path_len: usize,
        vars: *const u8,
        vars_len: usize,
        silent: c_int,
        tz: c_int,
        image_out: *mut *const u8,
        image_len: *mut usize,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonb_path_query_first(
        doc: *const u8,
        doc_len: usize,
        path: *const u8,
        path_len: usize,
        vars: *const u8,
        vars_len: usize,
        silent: c_int,
        tz: c_int,
        image_out: *mut *const u8,
        image_len: *mut usize,
        sqlstate_out: *mut c_int,
    ) -> c_int;
    fn pg_diff_jsonb_path_query_items(
        doc: *const u8,
        doc_len: usize,
        path: *const u8,
        path_len: usize,
        vars: *const u8,
        vars_len: usize,
        silent: c_int,
        tz: c_int,
        items_out: *mut *const u8,
        items_len: *mut usize,
        count_out: *mut c_int,
        sqlstate_out: *mut c_int,
    ) -> c_int;
}

/// Caps (recursion/carve rationale in the module header).
const MAX_PATH: usize = 256;
const MAX_DOC: usize = 512;
const MAX_VARS: usize = 256;

/// Carve accounting (smoke hit-rate; see README).
pub static EXEC_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static CARVE_HITS: AtomicU64 = AtomicU64::new(0);

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Tolerate sibling targets (jsonpath_diff, cryptofam, hashenc)
        // installing the IDENTICAL seam implementations first — all
        // oracles run in one test binary and seam_core::set panics on
        // double install (after swapping in the same impl, so catching is
        // sound).
        let _ = std::panic::catch_unwind(mbutils::init_seams);
        let _ = std::panic::catch_unwind(pg_locale::init_seams);
        pg_locale::set_default_locale_c_for_tests();
        // executeItemOptUnwrapTarget calls CHECK_FOR_INTERRUPTS every
        // recursion step; the C oracle's is a no-op (miscadmin.h shim) and
        // the Rust side gets the same no-interrupts model.
        let _ = std::panic::catch_unwind(|| {
            postgres_seams::check_for_interrupts::set(|| Ok(()));
        });
        adt_jsonpath_exec::init_seams();
    });
    let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
    if !pg_locale::default_locale_installed() {
        pg_locale::set_default_locale_c_for_tests();
    }
}

fn text_in_domain(text: &[u8], cap: usize) -> bool {
    text.len() <= cap && !text.contains(&0) && core::str::from_utf8(text).is_ok()
}

// ---------------------------------------------------------------------------
// Datetime carve: walk the PARSED item tree (mirrors path.rs print_item's
// child visits, so every reachable item is visited exactly once).
// ---------------------------------------------------------------------------

fn item_at<'a>(
    item: &adt_jsonpath::path::JsonPathItem<'a>,
    off: i32,
) -> adt_jsonpath::path::JsonPathItem<'a> {
    adt_jsonpath::path::jsp_init_by_buffer(item.buffer, item.base + off)
}

fn subtree_has_datetime(v: &adt_jsonpath::path::JsonPathItem<'_>) -> bool {
    let here = match v.typ {
        ItemType::Datetime
        | ItemType::Date
        | ItemType::Time
        | ItemType::TimeTz
        | ItemType::Timestamp
        | ItemType::TimestampTz => true,
        // binary operators: both args always present
        ItemType::And
        | ItemType::Or
        | ItemType::Equal
        | ItemType::NotEqual
        | ItemType::Less
        | ItemType::Greater
        | ItemType::LessOrEqual
        | ItemType::GreaterOrEqual
        | ItemType::Add
        | ItemType::Sub
        | ItemType::Mul
        | ItemType::Div
        | ItemType::Mod
        | ItemType::StartsWith => {
            subtree_has_datetime(&v.left_arg()) || subtree_has_datetime(&v.right_arg())
        }
        // unary operators: arg always present
        ItemType::Not
        | ItemType::IsUnknown
        | ItemType::Plus
        | ItemType::Minus
        | ItemType::Filter
        | ItemType::Exists => subtree_has_datetime(&v.arg()),
        ItemType::IndexArray => (0..v.content.array.nelems).any(|i| {
            let (from, to) = v.array_subscript(i);
            subtree_has_datetime(&from) || to.map_or(false, |t| subtree_has_datetime(&t))
        }),
        ItemType::LikeRegex => subtree_has_datetime(&item_at(v, v.content.like_regex.expr)),
        // .decimal(p,s): numeric literal args only — no datetime below
        _ => false,
    };
    if here {
        return true;
    }
    match v.next() {
        Some(n) => subtree_has_datetime(&n),
        None => false,
    }
}

fn path_has_datetime_item(image: &[u8]) -> bool {
    subtree_has_datetime(&adt_jsonpath::path::jsp_init(image))
}

// ---------------------------------------------------------------------------
// Verdicts + oracle wrappers
// ---------------------------------------------------------------------------

/// The result plane of one exec-arm call.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ExecVerdict {
    Bool(bool),
    /// SQL NULL result.
    Null,
    /// A jsonb image (query_array / query_first).
    Image(Vec<u8>),
    /// The SRF row set (query_items), each row a jsonb image.
    Rows(Vec<Vec<u8>>),
    Hard(i32),
}

fn c_last_msg() -> String {
    // SAFETY: live TLS NUL-terminated buffer.
    unsafe { std::ffi::CStr::from_ptr(pg_diff_jsonpath_last_msg()) }
        .to_string_lossy()
        .into_owned()
}

/// C jsonpath_in (hard mode): Ok(image) or Err(sqlstate).
fn c_path_in(text: &[u8]) -> Result<Vec<u8>, i32> {
    let mut img: *const u8 = core::ptr::null();
    let mut ilen: usize = 0;
    let mut st: c_int = 0;
    // SAFETY: live slice; out params are live locals.
    let rc = unsafe {
        pg_diff_jsonpath_in(text.as_ptr(), text.len(), 0, &mut img, &mut ilen, &mut st)
    };
    if rc == 0 {
        // SAFETY: arena image valid until the next pg_diff_* call.
        Ok(unsafe { core::slice::from_raw_parts(img, ilen) }.to_vec())
    } else {
        Err(st)
    }
}

struct COracle<'a> {
    doc: &'a [u8],
    path: &'a [u8],
    vars: &'a [u8],
    silent: bool,
    tz: bool,
}

impl COracle<'_> {
    fn bool_arm(&self, matcharm: bool, opr: bool) -> ExecVerdict {
        let f = if matcharm {
            pg_diff_jsonb_path_match
        } else {
            pg_diff_jsonb_path_exists
        };
        let mut res: c_int = -2;
        let mut st: c_int = 0;
        // SAFETY: all slices live; out params live locals.
        let rc = unsafe {
            f(
                self.doc.as_ptr(),
                self.doc.len(),
                self.path.as_ptr(),
                self.path.len(),
                self.vars.as_ptr(),
                self.vars.len(),
                self.silent as c_int,
                self.tz as c_int,
                opr as c_int,
                &mut res,
                &mut st,
            )
        };
        match rc {
            0 => ExecVerdict::Bool(res != 0),
            3 => ExecVerdict::Null,
            _ => ExecVerdict::Hard(st),
        }
    }

    fn image_arm(&self, first: bool) -> ExecVerdict {
        let f = if first {
            pg_diff_jsonb_path_query_first
        } else {
            pg_diff_jsonb_path_query_array
        };
        let mut img: *const u8 = core::ptr::null();
        let mut ilen: usize = 0;
        let mut st: c_int = 0;
        // SAFETY: all slices live; out params live locals.
        let rc = unsafe {
            f(
                self.doc.as_ptr(),
                self.doc.len(),
                self.path.as_ptr(),
                self.path.len(),
                self.vars.as_ptr(),
                self.vars.len(),
                self.silent as c_int,
                self.tz as c_int,
                &mut img,
                &mut ilen,
                &mut st,
            )
        };
        match rc {
            // SAFETY: arena image valid until the next pg_diff_* call.
            0 => ExecVerdict::Image(unsafe { core::slice::from_raw_parts(img, ilen) }.to_vec()),
            3 => ExecVerdict::Null,
            _ => ExecVerdict::Hard(st),
        }
    }

    fn items_arm(&self) -> ExecVerdict {
        let mut buf: *const u8 = core::ptr::null();
        let mut blen: usize = 0;
        let mut count: c_int = 0;
        let mut st: c_int = 0;
        // SAFETY: all slices live; out params live locals.
        let rc = unsafe {
            pg_diff_jsonb_path_query_items(
                self.doc.as_ptr(),
                self.doc.len(),
                self.path.as_ptr(),
                self.path.len(),
                self.vars.as_ptr(),
                self.vars.len(),
                self.silent as c_int,
                self.tz as c_int,
                &mut buf,
                &mut blen,
                &mut count,
                &mut st,
            )
        };
        if rc != 0 {
            return ExecVerdict::Hard(st);
        }
        // SAFETY: arena buffer valid until the next pg_diff_* call.
        let bytes = unsafe { core::slice::from_raw_parts(buf, blen) };
        let mut rows = Vec::with_capacity(count as usize);
        let mut off = 0usize;
        for _ in 0..count {
            let len =
                u32::from_ne_bytes(bytes[off..off + 4].try_into().expect("framed")) as usize;
            off += 4;
            rows.push(bytes[off..off + len].to_vec());
            off += len;
        }
        assert_eq!(off, blen, "oracle items buffer framing");
        ExecVerdict::Rows(rows)
    }
}

// ---------------------------------------------------------------------------
// Rust side: cores + fc-wrapper plane
// ---------------------------------------------------------------------------

/// jsonb payloads (headerless) for the cores; the fc plane gets full images.
struct RustArgs<'a> {
    doc_payload: &'a [u8],
    path_image: &'a [u8],
    vars_payload: Option<&'a [u8]>,
    silent: bool,
    tz: bool,
}

fn verdict_of_bool(r: PgResult<Option<bool>>) -> ExecVerdict {
    match r {
        Ok(Some(b)) => ExecVerdict::Bool(b),
        Ok(None) => ExecVerdict::Null,
        Err(e) => ExecVerdict::Hard(e.sqlstate().0),
    }
}

fn rust_core(arm: Arm, m: mcx::Mcx<'_>, a: &RustArgs<'_>) -> ExecVerdict {
    let vars = match a.vars_payload {
        Some(v) => JsonPathVars::Jsonb(v),
        None => JsonPathVars::None,
    };
    match arm {
        Arm::Exists | Arm::ExistsOpr => verdict_of_bool(
            adt_jsonpath_exec::jsonb_path_exists_core(
                m,
                a.doc_payload,
                a.path_image,
                vars,
                a.silent,
                a.tz,
            ),
        ),
        Arm::Match | Arm::MatchOpr => verdict_of_bool(
            adt_jsonpath_exec::jsonb_path_match_core(
                m,
                a.doc_payload,
                a.path_image,
                vars,
                a.silent,
                a.tz,
            ),
        ),
        Arm::QueryArray => match adt_jsonpath_exec::jsonb_path_query_array_core(
            m,
            a.doc_payload,
            a.path_image,
            vars,
            a.silent,
            a.tz,
        ) {
            Ok(v) => ExecVerdict::Image(v.to_vec()),
            Err(e) => ExecVerdict::Hard(e.sqlstate().0),
        },
        Arm::QueryFirst => match adt_jsonpath_exec::jsonb_path_query_first_core(
            m,
            a.doc_payload,
            a.path_image,
            vars,
            a.silent,
            a.tz,
        ) {
            Ok(Some(v)) => ExecVerdict::Image(v.to_vec()),
            Ok(None) => ExecVerdict::Null,
            Err(e) => ExecVerdict::Hard(e.sqlstate().0),
        },
        Arm::QueryItems => match adt_jsonpath_exec::jsonb_path_query_core(
            m,
            a.doc_payload,
            a.path_image,
            vars,
            a.silent,
            a.tz,
        ) {
            Ok(rows) => ExecVerdict::Rows(rows),
            Err(e) => ExecVerdict::Hard(e.sqlstate().0),
        },
    }
}

/// fc-wrapper plane: drive the catalog wrapper over a LocalFcinfo frame.
/// Returns None for arms whose wrapper is out of scope (query_items — the
/// SRF wrapper's MultiFuncCall plumbing is the documented carve).
fn rust_wrapper(
    arm: Arm,
    m: mcx::Mcx<'_>,
    doc_image: &[u8],
    path_image: &[u8],
    vars_image: &[u8],
    silent: bool,
    tz: bool,
) -> Option<ExecVerdict> {
    let b = |f: types_fmgr::PGFunction| {
        let mut fcinfo = LocalFcinfo::<4>::new(0);
        // SAFETY: the context owning `m` outlives this call (caller scope).
        unsafe { fcinfo.set_result_mcx(m) };
        fcinfo.args = [
            NullableDatum::value(Datum::from_usize(doc_image.as_ptr() as usize)),
            NullableDatum::value(Datum::from_usize(path_image.as_ptr() as usize)),
            NullableDatum::value(Datum::from_usize(vars_image.as_ptr() as usize)),
            NullableDatum::value(Datum::from_bool(silent)),
        ];
        let r = f(None, &mut fcinfo);
        (r, fcinfo.isnull)
    };
    let opr = |f: types_fmgr::PGFunction| {
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        // SAFETY: as above.
        unsafe { fcinfo.set_result_mcx(m) };
        fcinfo.args = [
            NullableDatum::value(Datum::from_usize(doc_image.as_ptr() as usize)),
            NullableDatum::value(Datum::from_usize(path_image.as_ptr() as usize)),
        ];
        let r = f(None, &mut fcinfo);
        (r, fcinfo.isnull)
    };

    use adt_jsonpath_exec::builtins as bi;
    let (r, isnull, image_result) = match (arm, tz) {
        (Arm::Exists, false) => {
            let (r, n) = b(bi::fc_jsonb_path_exists);
            (r, n, false)
        }
        (Arm::Exists, true) => {
            let (r, n) = b(bi::fc_jsonb_path_exists_tz);
            (r, n, false)
        }
        (Arm::Match, false) => {
            let (r, n) = b(bi::fc_jsonb_path_match);
            (r, n, false)
        }
        (Arm::Match, true) => {
            let (r, n) = b(bi::fc_jsonb_path_match_tz);
            (r, n, false)
        }
        (Arm::ExistsOpr, _) => {
            let (r, n) = opr(bi::fc_jsonb_path_exists_opr);
            (r, n, false)
        }
        (Arm::MatchOpr, _) => {
            let (r, n) = opr(bi::fc_jsonb_path_match_opr);
            (r, n, false)
        }
        (Arm::QueryArray, false) => {
            let (r, n) = b(bi::fc_jsonb_path_query_array);
            (r, n, true)
        }
        (Arm::QueryArray, true) => {
            let (r, n) = b(bi::fc_jsonb_path_query_array_tz);
            (r, n, true)
        }
        (Arm::QueryFirst, false) => {
            let (r, n) = b(bi::fc_jsonb_path_query_first);
            (r, n, true)
        }
        (Arm::QueryFirst, true) => {
            let (r, n) = b(bi::fc_jsonb_path_query_first_tz);
            (r, n, true)
        }
        (Arm::QueryItems, _) => return None,
    };
    Some(match r {
        Err(e) => ExecVerdict::Hard(e.sqlstate().0),
        Ok(_) if isnull => ExecVerdict::Null,
        Ok(d) if image_result => ExecVerdict::Image(datum_image(d).to_vec()),
        Ok(d) => ExecVerdict::Bool(d.as_usize() != 0),
    })
}

/// Re-frame a 4B-header varlena image as a 1-byte-header short varlena
/// (little-endian bit layout, as PG on this platform), when the payload fits.
fn short_frame(img: &[u8]) -> Option<Vec<u8>> {
    let payload = &img[4..];
    if payload.len() + 1 <= 0x7F {
        let mut s = Vec::with_capacity(payload.len() + 1);
        s.push((((payload.len() + 1) as u8) << 1) | 1);
        s.extend_from_slice(payload);
        Some(s)
    } else {
        None
    }
}

/// Read back a full 4B-header varlena image behind a by-ref result Datum.
fn datum_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: fc jsonb results are live 4B-header varlena images in the
    // armed arena, read before the arena drops.
    let hdr = unsafe { core::slice::from_raw_parts(p, 4) };
    let len = (u32::from_ne_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) >> 2) as usize;
    // SAFETY: readable through its full VARSIZE.
    unsafe { core::slice::from_raw_parts(p, len) }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Arm {
    Exists,
    Match,
    QueryArray,
    QueryFirst,
    ExistsOpr,
    MatchOpr,
    QueryItems,
}

const EMPTY_OBJECT: &[u8] = b"{}";

pub fn jsonpathexec_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.len() < 5 {
        return;
    }
    let sel = data[0];
    let s1 = u16::from_le_bytes([data[1], data[2]]) as usize;
    let s2 = u16::from_le_bytes([data[3], data[4]]) as usize;
    let rest = &data[5..];

    let arm = match sel & 0x07 {
        0 => Arm::Exists,
        1 => Arm::Match,
        2 => Arm::QueryArray,
        3 => Arm::QueryFirst,
        4 => Arm::ExistsOpr,
        5 => Arm::MatchOpr,
        6 => Arm::QueryItems,
        _ => Arm::Exists,
    };
    // opr arms are catalog-fixed: silent=true (implied), tz=false.
    let opr = matches!(arm, Arm::ExistsOpr | Arm::MatchOpr);
    let silent = if opr { true } else { (sel & 0x08) != 0 };
    let tz = if opr { false } else { (sel & 0x10) != 0 };
    let vars_present = !opr && (sel & 0x20) != 0;

    // Split rest into path / doc / vars.
    let o1 = s1 % (rest.len() + 1);
    let o2 = o1 + (s2 % (rest.len() - o1 + 1));
    let (path_text, doc_text, vars_text) = (&rest[..o1], &rest[o1..o2], &rest[o2..]);

    if !text_in_domain(path_text, MAX_PATH)
        || !text_in_domain(doc_text, MAX_DOC)
        || !text_in_domain(vars_text, MAX_VARS)
    {
        return;
    }

    setup();
    EXEC_TOTAL.fetch_add(1, Ordering::Relaxed);

    // ---- path parse gate (both sides; plane owned by jsonpath_diff) ----
    let cx = mcx::MemoryContext::new("jsonpathexec_fuzz");
    let m = cx.mcx();
    let rust_path = match adt_jsonpath::path::jsonpath_in(m, path_text, None) {
        Ok(Some(v)) => v,
        _ => return,
    };
    let Ok(c_path) = c_path_in(path_text) else {
        return;
    };
    if c_path.as_slice() != &rust_path[..] {
        // jsonpath_diff owns the parse plane; do not double-report here.
        return;
    }
    let path_image: &[u8] = &c_path;

    // ---- datetime carve (see module header) ----
    if path_has_datetime_item(path_image) {
        CARVE_HITS.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // ---- doc + vars parse (Rust adt_jsonb; strategy in module header) ----
    let doc_image = match adt_jsonb::io::jsonb_in(m, doc_text, None) {
        Ok(Some(v)) => v,
        _ => return,
    };
    let vars_image = if vars_present {
        match adt_jsonb::io::jsonb_in(m, vars_text, None) {
            Ok(Some(v)) => v,
            _ => return,
        }
    } else {
        match adt_jsonb::io::jsonb_in(m, EMPTY_OBJECT, None) {
            Ok(Some(v)) => v,
            _ => unreachable!("'{{}}' always parses"),
        }
    };

    run_exec_diff(
        arm,
        m,
        &doc_image,
        path_image,
        &vars_image,
        vars_present,
        silent,
        tz,
        path_text,
        doc_text,
        vars_text,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_exec_diff(
    arm: Arm,
    m: mcx::Mcx<'_>,
    doc_image: &[u8],
    path_image: &[u8],
    vars_image: &[u8],
    vars_present: bool,
    silent: bool,
    tz: bool,
    path_text: &[u8],
    doc_text: &[u8],
    vars_text: &[u8],
) {
    let opr = matches!(arm, Arm::ExistsOpr | Arm::MatchOpr);
    let ctx = || {
        format!(
            "arm={arm:?} silent={silent} tz={tz} vars_present={vars_present} path={:?} doc={:?} vars={:?}",
            String::from_utf8_lossy(path_text),
            String::from_utf8_lossy(doc_text),
            String::from_utf8_lossy(vars_text)
        )
    };

    // ---- C oracle ----
    let c = COracle {
        doc: doc_image,
        path: path_image,
        vars: vars_image,
        silent,
        tz,
    };
    let cv = match arm {
        Arm::Exists | Arm::ExistsOpr => c.bool_arm(false, opr),
        Arm::Match | Arm::MatchOpr => c.bool_arm(true, opr),
        Arm::QueryArray => c.image_arm(false),
        Arm::QueryFirst => c.image_arm(true),
        Arm::QueryItems => c.items_arm(),
    };
    let cmsg = c_last_msg();

    // ---- Rust core ----
    let a = RustArgs {
        doc_payload: &doc_image[4..],
        path_image,
        vars_payload: if opr { None } else { Some(&vars_image[4..]) },
        silent,
        tz,
    };
    let rv = rust_core(arm, m, &a);

    assert!(
        cv == rv,
        "jsonpathexec RESULT DIVERGENCE {}: C={cv:?} ({cmsg:?}) Rust={rv:?}",
        ctx()
    );

    // ---- fc-wrapper plane (wrapper vs core; C parity already carried) ----
    if let Some(wv) = rust_wrapper(arm, m, doc_image, path_image, vars_image, silent, tz) {
        assert!(
            wv == rv,
            "fc wrapper vs core DIVERGENCE {}: wrapper={wv:?} core={rv:?}",
            ctx()
        );
    }

    // ---- SHORT-VARLENA PLANE (mirrors jsonpath_diff arm 1) ----
    // arg_varlena (builtins.rs) mirrors PG_GETARG_JSONB_P's expansion of a
    // 1-byte-header short varlena into an aligned 4B-header copy. Everything
    // this harness builds is 4B-headed, so exercise the expansion
    // deliberately: re-frame each argument image as a short varlena (when it
    // fits) and require the fc wrapper to produce the identical verdict
    // through the short framings.
    let sdoc = short_frame(doc_image);
    let spath = short_frame(path_image);
    let svars = short_frame(vars_image);
    if sdoc.is_some() || spath.is_some() || svars.is_some() {
        if let Some(wv) = rust_wrapper(
            arm,
            m,
            sdoc.as_deref().unwrap_or(doc_image),
            spath.as_deref().unwrap_or(path_image),
            svars.as_deref().unwrap_or(vars_image),
            silent,
            tz,
        ) {
            assert!(
                wv == rv,
                "fc wrapper SHORT-VARLENA vs core DIVERGENCE {}: wrapper={wv:?} core={rv:?}",
                ctx()
            );
        }
    }

    // ---- JSON_EXISTS executor-entry plane (json_path_exists, List vars) ----
    // The PASSING-list model [(k, NULL), ...] is exactly equivalent to a vars
    // jsonb whose top-level values are ALL null: hits produce JbV::Null under
    // both models, misses raise the same undefined-object error, and the
    // base-object id difference (jsonb hit id=1 vs null-list hit id=0) is
    // unobservable through a null value (keyvalue, the only id consumer,
    // cannot apply to a null). Non-null values would require
    // json_item_from_datum (the claim's fmgr-datum carve), so the plane runs
    // only when the model fits; the exists core's C parity is already carried
    // by the exists arm above, making it the in-harness oracle here.
    let vp = &vars_image[4..];
    if adt_jsonb::container::container_is_object(vp)
        && !adt_jsonb::container::container_is_scalar(vp)
    {
        let mut names: Vec<&[u8]> = Vec::new();
        let mut all_null = true;
        if let Ok(mut it) = adt_jsonb::iter::JsonbIterator::init(m, vp) {
            loop {
                let (tok, item) = it.next(true);
                match tok {
                    adt_jsonb::iter::WjbToken::Done => break,
                    adt_jsonb::iter::WjbToken::Key => {
                        if let adt_jsonb::container::JsonbItem::String(k) = item {
                            names.push(k);
                        }
                    }
                    adt_jsonb::iter::WjbToken::Value => {
                        if !matches!(item, adt_jsonb::container::JsonbItem::Null) {
                            all_null = false;
                            break;
                        }
                    }
                    _ => {}
                }
            }
        } else {
            all_null = false;
        }
        if all_null {
            let list: Vec<adt_jsonpath_exec::JsonPathVariable> = names
                .iter()
                .map(|n| adt_jsonpath_exec::JsonPathVariable {
                    name: n,
                    typid: 0,
                    typmod: -1,
                    value: Datum::from_usize(0),
                    isnull: true,
                })
                .collect();
            let pv = match adt_jsonpath_exec::json_path_exists(
                m,
                &doc_image[4..],
                path_image,
                silent,
                &list,
            ) {
                Ok(Some(b)) => ExecVerdict::Bool(b),
                Ok(None) => ExecVerdict::Null,
                Err(e) => ExecVerdict::Hard(e.sqlstate().0),
            };
            let refv = verdict_of_bool(adt_jsonpath_exec::jsonb_path_exists_core(
                m,
                &doc_image[4..],
                path_image,
                JsonPathVars::Jsonb(vp),
                silent,
                true, // json_path_exists is use_tz=true by definition (C: JsonPathExists)
            ));
            assert!(
                pv == refv,
                "json_path_exists(List) vs exists core DIVERGENCE {}: entry={pv:?} core={refv:?}",
                ctx()
            );
        }
    }
}

/// Keep the builtins table linked so the catalog rows stay honest.
pub const _EXEC_BUILTINS: &[FmgrBuiltin] = adt_jsonpath_exec::builtins::JSONPATH_EXEC_BUILTINS;

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;

#[cfg(test)]
mod slow_unit_probe {
    /// TRIAGE PROBE (p1-laneaa): time the two sides separately on the fleet
    /// slow-unit artifact so the 37.9 s libFuzzer report is attributed to an
    /// engine rather than assumed. Driven by PGRUST_SLOW_UNIT=<file>.
    #[test]
    #[ignore = "triage probe: run explicitly with PGRUST_SLOW_UNIT set"]
    fn slow_unit_timing_probe() {
        let _serial = crate::c_oracle_serial();
        let Ok(f) = std::env::var("PGRUST_SLOW_UNIT") else { return };
        let data = std::fs::read(f).expect("slow unit file");
        let t = std::time::Instant::now();
        super::jsonpathexec_diff(&data);
        eprintln!("whole-iteration: {:?}", t.elapsed());
    }
}
