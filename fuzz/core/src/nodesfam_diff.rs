//! nodesfam_diff: differential fuzz driver for the three node-walker crates
//! vs verbatim vendored PostgreSQL 18.3 C (csrc/pg_nodesfam_io.c, upstream
//! sha 62d6c7d3df; lane p1-nodes):
//!
//!   crates/backend/nodes/readfuncs — stringToNode (read.c + readfuncs.c)
//!   crates/backend/nodes/outfuncs  — nodeToString (outfuncs.c)
//!   crates/backend/nodes/copyfuncs — copyObject   (copyfuncs.c)
//!
//! ONE fixture drives all three: the input is node-text (the outfuncs
//! serialization language). Pipeline per exec, both sides:
//!
//!     read(text) -> node
//!     out(node) -> text'                        [outfuncs vs _outNode]
//!     copy(node) -> node2; out(node2) -> text'' [copyfuncs vs copyObject]
//!     read(text') -> node3; out(node3)          [round-trip stability]
//!     C only: equal(node, copy)                 [equalfuncs witness]
//!
//! Comparison planes (all compared on every exec where both sides accept):
//!   P1 out-text bytes:      rust text' == C text'
//!   P2 copy self-oracle:    text'' == text' on EACH side independently
//!   P3 round-trip:          re-read/re-out == text' on EACH side
//!   P4 C equal(node, copy)  (C-side witness that the copy is structural)
//!   P5 verdict + errcode:   accept/reject agree; on structured errors the
//!      packed sqlstate matches (SqlState uses C's MAKE_SQLSTATE encoding,
//!      so C's int compares directly; e.g. 54001 stack-depth on both sides)
//!
//! SCOPE (the honest-verdict rules; see the tag census in tests.rs):
//!   The Rust crates are chartered SCOPED ports of the catalog-stored node
//!   universe (pg_rewrite ev_action / pg_attrdef adbin / pg_constraint
//!   conbin / pg_trigger tgqual): readfuncs dispatches 80 of C's 316
//!   labels, outfuncs 87 of C's 387 tags, copyfuncs 321 of C's 336. Out of
//!   scope, the ports PANIC BY CHARTER ("loud panic naming the C reader").
//!   Verdict table for one exec:
//!     C=err,  Rust=err/panic  -> PASS (both reject; errcode compared when
//!                                the Rust side is a structured PgError)
//!     C=ok,   Rust=ok         -> compare P1..P4
//!     C=err,  Rust=ok         -> DIVERGENCE (Rust accepted what C rejects)
//!     C=ok,   Rust=PgError    -> DIVERGENCE (structured over-rejection)
//!     C=ok,   Rust=panic:
//!         every {LABEL in the input is port-dispatched -> DIVERGENCE
//!           (in-scope panic: either a port hole or a value-scope carve
//!           that must be documented here — none are documented yet)
//!         any label outside the port set -> SCOPE CARVE (counted, OK;
//!           this is the chartered loud-panic arm)
//!
//! Value-node arm (selector): C reads bare `true`/`1.5`/`b101` tokens as
//! Boolean/Float/BitString value nodes; the Rust read port carries only
//! quoted strings + integers (list elements in the SELECT-rule universe).
//! The out/copy arms for Float/Boolean/String/Integer/BitString are still
//! port surface, so arm 1 BUILDS the value/list nodes programmatically
//! (types_nodes constructors over fuzz bytes), rust-outs them, and feeds
//! the text to the C pipeline: C read(text) must accept and re-out the
//! identical bytes, plus copy planes on both sides.
//!
//! Interior NUL: both sides get the input TRUNCATED at the first NUL (C
//! stringToNode is char*-terminated; feeding Rust the longer slice would
//! compare different inputs, the tzparser lesson).
//!
//! Recursion guard: C check_stack_depth is vendored REAL (stack_depth.c)
//! and pinned to the server-default 2048kB; the Rust side pins the same
//! via the stack_depth crate. THE GUARDS ARE PART OF THE SURFACE: the
//! p1-nodes lane ADDED the missing check_stack_depth calls to all three
//! Rust walkers (C parity, readfuncs.c:578 / outfuncs.c:733 /
//! copyfuncs.c:185) — before that fix, deep nesting crashed the process
//! where C raises 54001.

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::sync::OnceLock;

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_nodes::Node;

// stub:nodes — the bounded value/list node-tree builder is the shared
// constructed-state facility (fuzz/core/src/stub_nodes.rs); this target is
// the migration demo that consumes it.
use crate::stub_nodes::build_value_node;

/// Intern a &str into the context arena (the copyfuncs str_in shape).
fn intern<'m>(m: Mcx<'m>, s: &str) -> PgResult<&'m str> {
    let v = mcx::slice_in(m, s.as_bytes())?;
    // SAFETY: verbatim copy of a &str
    Ok(unsafe { core::str::from_utf8_unchecked(v.leak()) })
}

#[repr(C)]
struct NdfOut {
    verdict: c_int,   // 0 ok, 1 ereport(ERROR) captured
    errcode: c_int,   // packed sqlstate (MAKE_SQLSTATE encoding)
    out_text: *const c_char,
    copy_text: *const c_char,
    equal_ok: c_int,  // equal(node, copyObject(node))
    reread_ok: c_int, // out(read(out_text)) == out_text
}

extern "C" {
    fn pg_ndf_init();
    fn pg_ndf_exec(input: *const c_char) -> *const NdfOut;
}

/// C oracle verdict for one input text.
#[derive(Debug, PartialEq)]
enum COut {
    Ok { out: Vec<u8>, copy: Vec<u8>, equal_ok: bool, reread_ok: bool },
    Err { errcode: i32 },
}

fn c_exec(input: &[u8]) -> COut {
    rust_stack_init();
    let cs = CString::new(input).expect("caller truncates at NUL");
    unsafe {
        let r = &*pg_ndf_exec(cs.as_ptr());
        if r.verdict != 0 {
            return COut::Err { errcode: r.errcode };
        }
        let out = std::ffi::CStr::from_ptr(r.out_text).to_bytes().to_vec();
        let copy = std::ffi::CStr::from_ptr(r.copy_text).to_bytes().to_vec();
        COut::Ok { out, copy, equal_ok: r.equal_ok == 1, reread_ok: r.reread_ok == 1 }
    }
}

/// stub:nodes CONTROL HOOK (tests only): the C oracle's re-out of a node
/// text — the controls feed it a text describing a DIFFERENT tree than the
/// Rust builder produced and prove the re-out plane sees the difference.
#[cfg(test)]
pub(crate) fn c_reout_control(text: &[u8]) -> Result<Vec<u8>, i32> {
    match c_exec(text) {
        COut::Ok { out, .. } => Ok(out),
        COut::Err { errcode } => Err(errcode),
    }
}

/// Rust pipeline verdict for one input text.
enum ROut {
    Ok { out: Vec<u8>, copy: Vec<u8>, reread_ok: bool },
    /// Ok(None): the "<>" null-node marker.
    NullNode,
    Err { errcode: i32 },
    Panic { msg: String },
}

thread_local! {
    static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Arm the guards ONCE PER THREAD.
///
/// NOT once per process: C's `stack_base_ptr` is a process-global static and
/// Rust's is thread-local, so a base recorded on thread A is meaningless on
/// thread B — the computed depth becomes the distance between two unrelated
/// stacks, the guard fires on EVERY input, and the whole campaign silently
/// measures nothing but the depth-carve arm (the exact 2.5x-understatement
/// failure this campaign already root-caused once, ASan fake-stack edition).
/// Caught here by six tests going red the moment the process ran more than
/// one test thread.
fn rust_stack_init() {
    if !ARMED.get() {
        rearm_stack_bases();
        ARMED.set(true);
    }
}

/// Re-arm BOTH stack bases in the CALLING thread and pin max_stack_depth to
/// the server default 2048kB on both sides.
///
/// Why this is public and per-thread: C's `stack_base_ptr` is a process
/// static and Rust's is thread-local, so a base recorded on thread A is
/// meaningless on thread B — and the guard must be armed relative to the
/// stack the walkers actually recurse on. libFuzzer drives one thread, so
/// the OnceLock path above is right there; the deep-nesting test drives a
/// dedicated big-stack thread and calls this explicitly. A guard armed 2048kB
/// deep on a 2 MiB test-harness thread would be armed BEYOND the real stack
/// end — which is exactly how a "guard" silently becomes a stack overflow.
pub fn rearm_stack_bases() {
    unsafe { pg_ndf_init() };
    ARMED.set(true);
    stack_depth_core::set_stack_base();
    stack_depth_core::set_max_stack_depth(2048);
    stack_depth_core::assign_max_stack_depth(2048);
}

thread_local! {
    /// True only while the CHARTERED walker region is executing, i.e. while a
    /// loud-panic-by-charter is an EXPECTED outcome being classified.
    static IN_CHARTERED_REGION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// libfuzzer-sys installs a panic hook that calls `process::abort()` on EVERY
/// panic (lib.rs:92, so libFuzzer can walk the frames), which pre-empts
/// `catch_unwind` — under the fuzzer, the scoped ports' chartered loud panics
/// therefore killed the process instead of being classified. Witnessed on the
/// first local smoke leg: the committed value-token seed aborted at exec ~0
/// even though `cargo test` classified it as a carve.
///
/// Fix: a hook that stays silent ONLY inside the chartered region. Real
/// divergences panic OUTSIDE it, so they hit the default hook, print, and
/// still abort through libfuzzer-sys's own `catch_unwind` — a divergence is
/// still a crash artifact, exactly as the campaign requires.
fn install_chartered_panic_hook() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if IN_CHARTERED_REGION.get() {
                return; // expected, being classified by the caller
            }
            default_hook(info);
        }));
    });
}

fn rust_exec(input: &str) -> ROut {
    rust_stack_init();
    install_chartered_panic_hook();
    let input = input.to_owned();
    // The scoped ports panic BY CHARTER outside the catalog node universe;
    // comparator failures live OUTSIDE this catch so a divergence still
    // aborts the exec (fuzz artifact).
    IN_CHARTERED_REGION.set(true);
    let caught = std::panic::catch_unwind(move || -> Result<Option<(Vec<u8>, Vec<u8>, bool)>, Box<PgError>> {
        let cx = mcx::MemoryContext::new("nodesfam_fuzz");
        let m = cx.mcx();
        let Some(node) = readfuncs::stringToNodeNullable(m, &input)? else {
            return Ok(None);
        };
        let out1 = outfuncs::nodeToString(m, node)?;
        let copy = copyfuncs::copy_object(m, node)?;
        let out2 = outfuncs::nodeToString(m, copy)?;
        // Round-trip stability: re-read our own output, re-out it. MUST use
        // the NULLABLE entry: an empty List out-texts to "<>" (C's NIL and
        // C's NULL node are the SAME value — read.c returns (Node *) NIL —
        // so "<>" is a legitimate re-read input), and the non-nullable entry
        // panics on it by charter.
        let out3 = match readfuncs::stringToNodeNullable(m, out1.as_str())? {
            Some(node3) => outfuncs::nodeToString(m, node3)?.as_str().to_owned(),
            None => "<>".to_owned(),
        };
        let reread_ok = out1.as_str() == out3;
        Ok(Some((
            out1.as_str().as_bytes().to_vec(),
            out2.as_str().as_bytes().to_vec(),
            reread_ok,
        )))
    });
    IN_CHARTERED_REGION.set(false);
    match caught {
        Ok(Ok(Some((out, copy, reread_ok)))) => ROut::Ok { out, copy, reread_ok },
        Ok(Ok(None)) => ROut::NullNode,
        Ok(Err(e)) => ROut::Err { errcode: e.sqlstate().0 },
        Err(payload) => ROut::Panic { msg: panic_msg(&payload) },
    }
}

fn panic_msg(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_owned()
    }
}

/// The port's read dispatch set, extracted at compile time from the crate
/// source of truth (the `b"LABEL" => self.read_*` arms). tests.rs asserts
/// this parse is exact and a subset of the C switch.
pub fn port_read_labels() -> &'static Vec<&'static str> {
    static LABELS: OnceLock<Vec<&'static str>> = OnceLock::new();
    LABELS.get_or_init(|| {
        let src: &str = include_str!("../../../crates/backend/nodes/readfuncs/src/lib.rs");
        let mut v = Vec::new();
        for line in src.lines() {
            let t = line.trim();
            if let Some(rest) = t.strip_prefix("b\"") {
                if let Some(idx) = rest.find("\" => self.read_") {
                    v.push(&rest[..idx]);
                }
            }
        }
        v.sort_unstable();
        v.dedup();
        assert!(
            v.len() >= 70,
            "port_read_labels parse collapsed ({} labels) — dispatch regex drifted",
            v.len()
        );
        v
    })
}

/// C readfuncs switch label set, from the vendored GENERATED switch file.
pub fn c_read_labels() -> &'static Vec<&'static str> {
    static LABELS: OnceLock<Vec<&'static str>> = OnceLock::new();
    LABELS.get_or_init(|| {
        let src: &str = include_str!("../csrc/nodesfam/gen/readfuncs.switch.c");
        let mut v = Vec::new();
        for line in src.lines() {
            if let Some(rest) = line.trim().strip_prefix("if (MATCH(\"") {
                if let Some(idx) = rest.find('"') {
                    v.push(&rest[..idx]);
                }
            }
        }
        v.sort_unstable();
        v.dedup();
        assert!(v.len() > 300, "c_read_labels parse collapsed: {}", v.len());
        v
    })
}


/// EXPECTED FIELD SEQUENCE per C node label, parsed from the GENERATED C
/// reader bodies (csrc/nodesfam/gen/readfuncs.funcs.c) plus the hand-written
/// readers in csrc/nodesfam/src/readfuncs.c.
///
/// WHY THIS GATE EXISTS (harness finding of record, lane p1-nodes): C's node
/// readers are NOT hardened against malformed text — catalog node strings are
/// written by C's own outfuncs and therefore TRUSTED. Only a handful of shapes
/// elog; a wrong field NAME or a missing field walks the reader off the token
/// stream and dereferences garbage. Feeding libFuzzer's raw mutations straight
/// to the oracle SIGSEGVs the C side (witnessed: `{CREATESTMT :relation <>}`,
/// a 1-of-13-fields truncation, segfaults inside _readCreateStmt), and those
/// crashes are neither pgrust defects nor meaningful upstream defects.
///
/// So the COMPARED DOMAIN is well-formed node text: for every `{LABEL ...}`
/// block whose LABEL C knows, the field-name sequence must equal C's expected
/// sequence exactly. Field VALUES, nesting, list contents, escaping and
/// whitespace stay fully free for the fuzzer — that is where the interesting
/// surface is. Unknown labels pass the gate untouched (C's parseNodeString
/// elogs "badly formatted node string" before touching any field, which is a
/// compared error-verdict, not a crash).
fn expected_fields() -> &'static std::collections::HashMap<String, Vec<(String, String)>> {
    static MAP: OnceLock<std::collections::HashMap<String, Vec<(String, String)>>> =
        OnceLock::new();
    MAP.get_or_init(|| {
        // fn name -> ordered (field, macro-kind) list
        let mut bodies: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for src in [
            include_str!("../csrc/nodesfam/gen/readfuncs.funcs.c"),
            include_str!("../csrc/nodesfam/src/readfuncs.c"),
        ] {
            let mut cur: Option<String> = None;
            for line in src.lines() {
                if let Some(rest) = line.strip_prefix("_read") {
                    if let Some(i) = rest.find("(void)") {
                        cur = Some(format!("_read{}", &rest[..i]));
                        bodies.entry(cur.clone().unwrap()).or_default();
                        continue;
                    }
                }
                if line == "}" {
                    cur = None;
                    continue;
                }
                let t = line.trim();
                if let (Some(f), Some(open)) = (cur.as_ref(), t.find('(')) {
                    if t.starts_with("READ_") && !t.starts_with("READ_LOCALS")
                        && !t.starts_with("READ_TEMP_LOCALS") && !t.starts_with("READ_DONE")
                    {
                        let mut kind: String = t[..open].trim().to_owned();
                        if kind == "READ_ENUM_FIELD" {
                            // READ_ENUM_FIELD(fldname, EnumType): keep the type
                            if let Some(comma) = t[open..].find(',') {
                                let ty: String = t[open + comma + 1..]
                                    .trim_start()
                                    .chars()
                                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                                    .collect();
                                if !ty.is_empty() {
                                    kind = format!("READ_ENUM_FIELD:{ty}");
                                }
                            }
                        }
                        let inner = &t[open + 1..];
                        let name: String = inner
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect();
                        if !name.is_empty() {
                            bodies.get_mut(f).expect("body").push((name, kind));
                        }
                    }
                }
            }
        }
        // label -> fn, from the generated switch. The MATCH test and the
        // _read call sit on CONSECUTIVE lines, so pair them with lookahead.
        let mut out = std::collections::HashMap::new();
        let sw: Vec<&str> = include_str!("../csrc/nodesfam/gen/readfuncs.switch.c")
            .lines()
            .collect();
        for (k, line) in sw.iter().enumerate() {
            let t = line.trim();
            let Some(rest) = t.strip_prefix("if (MATCH(\"") else { continue };
            let Some(i) = rest.find('"') else { continue };
            let label = &rest[..i];
            let Some(next) = sw.get(k + 1) else { continue };
            let Some(j) = next.find("_read") else { continue };
            let fname: String = next[j..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if let Some(fields) = bodies.get(&fname) {
                out.insert(label.to_string(), fields.clone());
            }
        }
        assert!(out.len() > 250, "field-sequence map collapsed: {}", out.len());
        out
    })
}

/// The 6 node labels whose C reader is HAND-WRITTEN (not generated) and whose
/// field sequence is therefore CONDITIONAL — `_readRangeTblEntry` switches on
/// rtekind, `_readA_Expr` on kind, `_readConst`/`_readBoolExpr`/`_readA_Const`/
/// `_readExtensibleNode` consume fields with raw `pg_strtok`. No static
/// sequence models them, so they are gated against the field-name sequences
/// OBSERVED IN THE C-VALIDATED SEED CORPUS (values, nesting and escaping stay
/// free; only the shape is pinned). Adding an rtekind variant = adding a
/// validated seed. `custom_reader_labels_match_the_c_source` keeps this list
/// equal to the hand-written reader set.
pub const CUSTOM_READER_LABELS: &[&str] = &[
    "BOOLEXPR", "CONST", "RANGETBLENTRY", "A_CONST", "A_EXPR", "EXTENSIBLENODE",
];

/// field -> macro-kind for the CUSTOM (hand-written) readers, collected from
/// ALL branches of their bodies. Their field SEQUENCE is conditional (so it is
/// gated against corpus shapes), but each field's KIND is fixed, which is what
/// the value check needs. Without this, values inside a custom block went
/// unvalidated: `{RANGETBLENTRY ... :rtekind \x06 ...}` reached the oracle,
/// where C's atoi swallows the control byte as 0 and the port panics.
fn custom_field_kinds() -> &'static std::collections::HashMap<String, String> {
    static MAP: OnceLock<std::collections::HashMap<String, String>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut out = std::collections::HashMap::new();
        for line in include_str!("../csrc/nodesfam/src/readfuncs.c").lines() {
            let t = line.trim();
            if !t.starts_with("READ_") || t.starts_with("READ_LOCALS") || t.starts_with("READ_DONE")
            {
                continue;
            }
            let Some(open) = t.find('(') else { continue };
            let mut kind = t[..open].trim().to_owned();
            let inner = &t[open + 1..];
            let name: String = inner
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                continue;
            }
            if kind == "READ_ENUM_FIELD" {
                if let Some(comma) = t[open..].find(',') {
                    let ty: String = t[open + comma + 1..]
                        .trim_start()
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect();
                    if !ty.is_empty() {
                        kind = format!("READ_ENUM_FIELD:{ty}");
                    }
                }
            }
            out.insert(name, kind);
        }
        assert!(out.len() > 20, "custom_field_kinds parse collapsed: {}", out.len());
        out
    })
}

/// Field-name sequences for the custom-reader labels, learned from the
/// committed corpus (each seed was validated against the C oracle before it
/// was written).
fn custom_shapes() -> &'static std::collections::HashMap<String, Vec<Vec<String>>> {
    static MAP: OnceLock<std::collections::HashMap<String, Vec<Vec<String>>>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut out: std::collections::HashMap<String, Vec<Vec<String>>> =
            std::collections::HashMap::new();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../corpus/nodesfam_diff");
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for e in rd.flatten() {
                // CURATED SEEDS ONLY (`seed-*`): every one was validated
                // against the C oracle before it was committed. Learning
                // shapes from FUZZER-GROWN corpus entries self-poisons the
                // gate — witnessed: libFuzzer wrote
                // `{BOOLEXPR :boolop and :args <> :locatiol -1}` into the
                // corpus, the gate then accepted that misspelled field name
                // as a legal BOOLEXPR shape, and the harness reported the
                // resulting (C-accepts / port-verifies) panic as a divergence.
                if !e.file_name().to_string_lossy().starts_with("seed-") {
                    continue;
                }
                let Ok(data) = std::fs::read(e.path()) else { continue };
                let body = if data.first() == Some(&0) { &data[1..] } else { &data[..] };
                let Ok(text) = std::str::from_utf8(body) else { continue };
                for (label, fields) in text_blocks(text) {
                    if CUSTOM_READER_LABELS.contains(&label.as_str()) {
                        out.entry(label).or_default().push(fields);
                    }
                }
            }
        }
        for v in out.values_mut() {
            v.sort();
            v.dedup();
        }
        out
    })
}

/// Every `{LABEL :field ... }` block in the text as (label, field-name-seq),
/// over the pg_strtok token stream. A field is a token starting with `:`
/// IMMEDIATELY at a field slot; nested blocks are reported separately.
fn text_blocks(text: &str) -> Vec<(String, Vec<String>)> {
    let toks = pg_strtok_all(text);
    let mut done: Vec<(String, Vec<String>)> = Vec::new();
    let mut stack: Vec<(String, Vec<String>)> = Vec::new();
    let mut k = 0;
    while k < toks.len() {
        let t = toks[k];
        if t == "{" {
            let label = toks.get(k + 1).copied().unwrap_or("");
            stack.push((label.to_owned(), Vec::new()));
            k += 2;
            continue;
        }
        if t == "}" {
            if let Some(b) = stack.pop() {
                done.push(b);
            }
            k += 1;
            continue;
        }
        if let Some(name) = t.strip_prefix(':') {
            // DISCRIMINANT-AWARE SHAPE KEY: a custom reader's field sequence
            // depends on an enum field's VALUE (_readRangeTblEntry switches on
            // rtekind, _readA_Expr on kind), so the value joins the key.
            // Without this, `:rtekind 6` (RTE_CTE) with a relation-shaped body
            // matched the relation shape, C read its CTE branch happily (its
            // READ macros do not verify field names) and the port panicked.
            let entry = match custom_field_kinds().get(name) {
                Some(kind) if kind.starts_with("READ_ENUM_FIELD:") => {
                    format!("{name}={}", toks.get(k + 1).copied().unwrap_or(""))
                }
                _ => name.to_owned(),
            };
            if let Some(top) = stack.last_mut() {
                top.1.push(entry);
            }
        }
        k += 1;
    }
    done
}

/// C `readDatum` (readfuncs.c) token grammar, modelled so the gate can reject
/// payloads that NULL-deref the verbatim C reader.
///
/// readDatum reads `<length>` then `[`, then — for a BYVAL datum —
/// `sizeof(Datum)` = 8 byte tokens regardless of length, or `length` tokens
/// for byref, then `]`. Every byte token goes through `atoi(token)` with NO
/// NULL CHECK, so a payload with fewer tokens than required makes pg_strtok
/// return NULL and C segfaults inside strtol (witnessed:
/// `... :constvalue 1 [ 1 0 0 0alias0 0 ]}` — `0alias0` is one token, so the
/// stream runs one byte short). Third instance of the un-hardened-C-reader
/// class in this family, after the truncated-CreateStmt SEGV and the
/// non-verifying field-name slots.
fn datum_payload_is_well_formed(toks: &[&str], start: usize, byval: bool, length: i64) -> bool {
    // toks[start] is the token AFTER :constvalue's length token, i.e. `[`
    if toks.get(start).copied() != Some("[") {
        return false;
    }
    let want = if byval {
        8 // sizeof(Datum) on LP64, read unconditionally
    } else if length <= 0 {
        0
    } else {
        length as usize
    };
    let mut k = start + 1;
    for _ in 0..want {
        match toks.get(k) {
            // C does atoi(token) with no NULL check; outfuncs writes each byte
            // as a plain %d, so a proper decimal (one optional leading '-',
            // then at least one digit) is the only producible form — a lone
            // `-` reached the oracle before this, where atoi("-") is 0.
            Some(t) => {
                let body = t.strip_prefix('-').unwrap_or(t);
                if body.is_empty() || !body.bytes().all(|b| b.is_ascii_digit()) {
                    return false;
                }
            }
            None => return false,
        }
        k += 1;
    }
    toks.get(k).copied() == Some("]")
}

/// Locate every CONST block and validate its datum payload against
/// `readDatum`'s grammar. Uses the block's own :constbyval / :constlen /
/// :constisnull tokens, exactly as C's `_readConst` does.
fn const_datums_are_well_formed(text: &str) -> bool {
    let toks = pg_strtok_all(text);
    let mut k = 0;
    while k < toks.len() {
        if toks[k] == "{" && toks.get(k + 1).copied() == Some("CONST") {
            // scan this block's field tokens
            let mut byval = None;
            let mut isnull = None;
            let mut j = k + 2;
            let mut depth = 1usize;
            while j < toks.len() {
                match toks[j] {
                    "{" => depth += 1,
                    "}" => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    ":constbyval" => byval = toks.get(j + 1).map(|t| *t == "true"),
                    ":constisnull" => isnull = toks.get(j + 1).map(|t| *t == "true"),
                    ":constvalue" => {
                        // A NULL Const's value is written as exactly "<>";
                        // C's _readConst skips the token WITHOUT checking it,
                        // so any garbage is accepted there while the port
                        // asserts the marker (witness: ":constvalue <,").
                        if isnull == Some(true) {
                            return toks.get(j + 1).copied() == Some("<>");
                        }
                        let Some(lt) = toks.get(j + 1) else { return false };
                        let Ok(len) = lt.parse::<i64>() else { return false };
                        let Some(bv) = byval else { return false };
                        if !datum_payload_is_well_formed(&toks, j + 2, bv, len) {
                            return false;
                        }
                        break;
                    }
                    _ => {}
                }
                j += 1;
            }
        }
        k += 1;
    }
    true
}

/// DIVERGENCE OF RECORD — MATCH-OR-FIX RULING OWED (lane p1-nodes).
///
/// `GroupingSet.content`: the pgrust writer picks the list FLAVOR from `kind`
/// (`out_grouping_set`: emit `(i ...)` iff kind == GROUPING_SET_SIMPLE, else
/// `out_list`), while C's `outNode` picks it from the LIST'S ACTUAL TAG. The
/// mismatch cuts both ways, and the fuzzer found both directions:
///
///   `:kind 1 :content (14)`     C -> `(14)`,     pgrust -> `(i 14)`
///   `:kind 0 :content (i 14)`   C -> `(i 14)`,   pgrust -> `(14)`
///
/// Root cause is one line of the port: flavor is inferred from a sibling field
/// instead of carried by the value. Both spellings are UNREACHABLE from
/// PG-written text — the rewriter stores an `IntList` exactly when kind is
/// SIMPLE, NIL (`<>`) for EMPTY, and a List of GroupingSet nodes for
/// ROLLUP/CUBE/SETS — so the compared domain excludes the mismatched
/// combinations, and the writer-produced ones round-trip identically on both
/// sides (seed `seed-groupingset-intlist`). Fixing it properly means carrying
/// the IntList-vs-List distinction in the vocabulary, a port change outside
/// this lane's mandate; hence a RULING, not a silent carve.
///
/// The gate below encodes the WRITER's rule: content may be `<>` always;
/// `(i ...)` only under kind 1; a node list only under kinds 2/3/4.
fn groupingset_content_is_writer_producible(text: &str) -> bool {
    let toks = pg_strtok_all(text);
    let mut k = 0;
    while k < toks.len() {
        if toks[k] == "{" && toks.get(k + 1).copied() == Some("GROUPINGSET") {
            let mut kind: Option<i64> = None;
            let mut j = k + 2;
            let mut depth = 1usize;
            while j < toks.len() {
                match toks[j] {
                    "{" => depth += 1,
                    "}" => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    ":kind" if depth == 1 => {
                        kind = toks.get(j + 1).and_then(|t| t.parse::<i64>().ok());
                    }
                    ":content" if depth == 1 => {
                        let v = toks.get(j + 1).copied();
                        match v {
                            Some("<>") => {}
                            Some("(") => {
                                let marked = toks.get(j + 2).copied() == Some("i");
                                match kind {
                                    Some(1) if marked => {}
                                    Some(2) | Some(3) | Some(4) if !marked => {}
                                    _ => return false,
                                }
                            }
                            _ => return false,
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
        }
        k += 1;
    }
    true
}

fn is_well_formed(text: &str) -> bool {
    if !groupingset_content_is_writer_producible(text) {
        return false;
    }
    if !const_datums_are_well_formed(text) {
        return false;
    }
    // custom-reader labels: shape must match a corpus-validated sequence
    for (label, fields) in text_blocks(text) {
        if CUSTOM_READER_LABELS.contains(&label.as_str()) {
            match custom_shapes().get(&label) {
                Some(shapes) if shapes.iter().any(|s| *s == fields) => {}
                _ => return false,
            }
        }
    }
    well_formed_token_stream(text)
}

/// TOKEN-STREAM well-formedness: walk the stream the way C's readers do and
/// require that at every field slot the token is EXACTLY `:expected`.
///
/// This models what C's `READ_*_FIELD` macros actually do — they `pg_strtok`
/// once to SKIP the field name WITHOUT COMPARING IT (readfuncs.c macro
/// bodies: `/* skip :fldname */`) and once to take the value. So C blindly
/// accepts any garbage token in a field-name slot, while the Rust port
/// verifies the name and panics on a mismatch. That asymmetry is a real
/// permissiveness delta, but it is only reachable from text no PG writer
/// emits, so the compared domain excludes it (finding recorded in the lane
/// report; witnessed by `{GROUPINGSET :kind 0 :content <> K:location -1 }`).
/// Is a BARE token (a list element or a top-level value, i.e. not a field name
/// and not a field value) one that C's own writer could emit?
///
/// outfuncs writes exactly: `<>` for NULL, `"..."` for strings (with
/// outToken's backslash escapes), `%d` integers, shortest-decimal floats,
/// `true`/`false`, and `b<bits>` bitstrings — plus the structural tokens.
/// C's `nodeTokenType` classifies by LEADING character and then parses with
/// `atoi`/`strtol`, so `8A` reads as the integer 8 while the Rust port
/// validates the whole token and panics; same atoi-prefix permissiveness as
/// the field-kind case, so it is gated the same way.
fn bare_token_producible(tok: &str) -> bool {
    if matches!(tok, "{" | "}" | "(" | ")" | "<>" | "true" | "false") {
        return true;
    }
    // list-kind markers: (i ...) (o ...) (x ...) (b ...)
    if matches!(tok, "i" | "o" | "x" | "b") {
        return true;
    }
    let bytes = tok.as_bytes();
    // quoted string token (nodeTokenType: leading and trailing '"')
    if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
        return true;
    }
    // bitstring: C nodeTokenType takes ANY token starting 'b' or 'x'
    if bytes[0] == b'b' || bytes[0] == b'x' {
        return true;
    }
    // numeric: C's nodeTokenType takes a leading digit/sign/dot, then the
    // writer only ever emits a fully-valid integer or float
    let lead_numeric = bytes[0].is_ascii_digit()
        || ((bytes[0] == b'-' || bytes[0] == b'+' || bytes[0] == b'.') && bytes.len() > 1);
    if lead_numeric {
        let body = tok.strip_prefix('-').unwrap_or(tok);
        let body = body.strip_prefix('+').unwrap_or(body);
        return body.bytes().all(|c| c.is_ascii_digit()) || tok.parse::<f64>().is_ok();
    }
    // an unquoted word: outfuncs emits these only for enum-ish spellings the
    // hand-written readers consume as field VALUES (handled at field slots),
    // never as a bare list element.
    false
}

fn well_formed_token_stream(text: &str) -> bool {
    let toks = pg_strtok_all(text);
    let mut k = 0;
    // the text is a sequence of top-level values (normally exactly one)
    while k < toks.len() {
        match parse_value(&toks, k) {
            Some(next) => {
                debug_assert!(next > k);
                k = next;
            }
            None => return false,
        }
    }
    true
}

/// One node-text VALUE: `<>`, a bare token, a `{...}` block, or a `(...)` list.
/// Returns the index just past it, or None if it is not writer-producible.
fn parse_value(toks: &[&str], k: usize) -> Option<usize> {
    match toks.get(k).copied()? {
        "{" => parse_block(toks, k),
        "(" => parse_list(toks, k),
        ")" | "}" => None, // unbalanced
        t if bare_token_producible(t) => Some(k + 1),
        _ => None,
    }
}

/// `( ... )`: an optional kind marker (`i`/`o`/`x`/`b`) then values.
fn parse_list(toks: &[&str], k: usize) -> Option<usize> {
    debug_assert_eq!(toks[k], "(");
    let mut j = k + 1;
    // typed lists carry a one-letter marker whose elements are plain ints
    if matches!(toks.get(j).copied(), Some("i") | Some("o") | Some("x") | Some("b")) {
        j += 1;
        while let Some(t) = toks.get(j).copied() {
            if t == ")" {
                return Some(j + 1);
            }
            // C reads these with strtol/atoi; the writer emits decimals
            if t.is_empty() || !t.bytes().all(|b| b.is_ascii_digit() || b == b'-') {
                return None;
            }
            j += 1;
        }
        return None;
    }
    loop {
        match toks.get(j).copied()? {
            ")" => return Some(j + 1),
            _ => j = parse_value(toks, j)?,
        }
    }
}

/// `{LABEL :field value ... }`, validated against C's expected field sequence
/// for that label (or, for a CUSTOM reader, consumed as a whole because its
/// shape is checked against the C-validated corpus shapes elsewhere).
fn parse_block(toks: &[&str], k: usize) -> Option<usize> {
    debug_assert_eq!(toks[k], "{");
    let label = toks.get(k + 1).copied()?;
    if label == "}" {
        return None;
    }
    let mut j = k + 2;
    if CUSTOM_READER_LABELS.contains(&label) || !expected_fields().contains_key(label) {
        // custom reader, or a label C does not know (C's parseNodeString
        // elogs before touching a field, a compared error verdict): consume
        // the block, keeping brace/paren balance.
        let known_label = CUSTOM_READER_LABELS.contains(&label);
        // STRICT ALTERNATION at this level: every token is either `:field`
        // followed by exactly one value, or the closing `}`. A stray token
        // shifts C's token stream by one, so C then reads a field NAME as a
        // VALUE (its READ macros never verify names) and walks off into a NULL
        // deref — witnessed: `... :colnames ("a")}2 :rtekind 8 ...` SEGVs
        // inside strtoul. Values may be nested structures, which recurse.
        loop {
            match toks.get(j).copied()? {
                "}" => return Some(j + 1),
                t => {
                    let fname = t.strip_prefix(':')?;
                    j += 1;
                    // constvalue's payload is validated by
                    // const_datums_are_well_formed (it is not a single value)
                    if fname == "constvalue" {
                        // readDatum's payload SHAPE (length, `[`, decimal byte
                        // tokens, `]`) or the `<>` NULL marker. A loose
                        // skip-to-`]`-or-`}` used to swallow arbitrary
                        // corrupted content and let a shape through that
                        // SEGV'd _readRangeTblEntry; the exact byte COUNT is
                        // enforced by const_datums_are_well_formed, which
                        // knows the block's constbyval/constisnull.
                        if toks.get(j).copied() == Some("<>") {
                            j += 1;
                            continue;
                        }
                        let lt = toks.get(j).copied()?;
                        if lt.is_empty() || !lt.bytes().all(|b| b.is_ascii_digit()) {
                            return None;
                        }
                        j += 1;
                        if toks.get(j).copied()? != "[" {
                            return None;
                        }
                        j += 1;
                        loop {
                            match toks.get(j).copied()? {
                                "]" => {
                                    j += 1;
                                    break;
                                }
                                t => {
                                    let body = t.strip_prefix('-').unwrap_or(t);
                                    if body.is_empty()
                                        || !body.bytes().all(|b| b.is_ascii_digit())
                                    {
                                        return None;
                                    }
                                    j += 1;
                                }
                            }
                        }
                        continue;
                    }
                    if known_label {
                        if let Some(kind) = custom_field_kinds().get(fname) {
                            let v = toks.get(j).copied()?;
                            if !value_token_matches_kind(v, kind) {
                                return None;
                            }
                        }
                    }
                    // a field VALUE is a nested structure or ONE token; unlike
                    // a list element it may be a bare word (CHAR/STRING kinds:
                    // outToken writes `r`, `c`, escaped words), so this must
                    // NOT go through the list-element producibility rule.
                    match toks.get(j).copied()? {
                        "{" => j = parse_block(toks, j)?,
                        "(" => j = parse_list(toks, j)?,
                        ")" | "}" => return None,
                        _ => j += 1,
                    }
                }
            }
        }
    }
    let fields = expected_fields().get(label)?;
    for (fname, kind) in fields {
        if toks.get(j).copied()? != format!(":{fname}") {
            return None;
        }
        j += 1;
        // the value: a nested structure, or one kind-checked token
        match toks.get(j).copied()? {
            "{" => j = parse_block(toks, j)?,
            "(" => {
                if !value_token_matches_kind("(", kind) {
                    return None;
                }
                j = parse_list(toks, j)?;
            }
            t => {
                if !value_token_matches_kind(t, kind) {
                    return None;
                }
                j += 1;
            }
        }
    }
    if toks.get(j).copied()? != "}" {
        return None;
    }
    Some(j + 1)
}

/// Does a value token have the LEXICAL FORM C's reader macro for this field
/// kind would produce on the writing side?
///
/// WHY THE GATE CHECKS THIS (finding of record): C's numeric READ macros use
/// `atoi`/`atooid`/`atol`/`strtod`, which parse a PREFIX and silently ignore
/// trailing garbage — `atoi("0`")` is 0 — while the Rust port validates the
/// whole token and panics ("readfuncs.c: bad integer token"). Rather than let
/// that permissiveness delta accumulate as carve after carve, the compared
/// domain is restricted to tokens C's own writer could emit for the field's
/// kind. The delta itself is recorded in the lane report.
/// Writer-producible integer values per C enum type, from the GENERATED
/// `gen/enum_domains.tsv` (gen_enum_domains.py over the vendored header
/// closure). `*` = the parser would not model the block; those enums stay
/// permissive and their divergences are handled by the panic-carve classes.
fn enum_domains() -> &'static std::collections::HashMap<String, Option<Vec<i64>>> {
    static MAP: OnceLock<std::collections::HashMap<String, Option<Vec<i64>>>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut out = std::collections::HashMap::new();
        for line in include_str!("../csrc/nodesfam/gen/enum_domains.tsv").lines() {
            if line.starts_with('#') {
                continue;
            }
            let mut it = line.splitn(2, '\t');
            let (Some(name), Some(vals)) = (it.next(), it.next()) else { continue };
            let parsed = if vals.trim() == "*" {
                None
            } else {
                Some(vals.split(',').filter_map(|v| v.trim().parse::<i64>().ok()).collect())
            };
            out.insert(name.to_owned(), parsed);
        }
        assert!(out.len() > 100, "enum_domains.tsv parse collapsed: {}", out.len());
        out
    })
}

fn value_token_matches_kind(tok: &str, kind: &str) -> bool {
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let signed_int = |s: &str| {
        let body = s.strip_prefix('-').unwrap_or(s);
        all_digits(body)
    };
    // READ_ENUM_FIELD carries its C enum type: the value must be one the
    // enum actually declares, because an out-of-domain enum integer is not
    // writer-producible and the two sides disagree on it in TWO ways — the
    // port either panics (24 validators) or SILENTLY maps to the default and
    // loses the value (witnessed: `{JSONFORMAT :format_type 5 ...}`, C echoes
    // 5, pgrust echoes 0 — an OUT-TEXT divergence, not a panic).
    if let Some(ty) = kind.strip_prefix("READ_ENUM_FIELD:") {
        if !signed_int(tok) {
            return false;
        }
        let Ok(v) = tok.parse::<i64>() else { return false };
        return match enum_domains().get(ty) {
            Some(Some(vals)) => vals.contains(&v),
            // unmodelled or unknown enum: permissive (carve classes cover it)
            _ => true,
        };
    }
    match kind {
        "READ_INT_FIELD" | "READ_LONG_FIELD" | "READ_INT64_FIELD" | "READ_ENUM_FIELD"
        | "READ_LOCATION_FIELD" => signed_int(tok),
        "READ_UINT_FIELD" | "READ_UINT64_FIELD" | "READ_OID_FIELD" => all_digits(tok),
        "READ_BOOL_FIELD" => tok == "true" || tok == "false",
        // outfuncs writes a char as one byte (or as \0-escaped); C reads
        // token[0], so any single-byte token is writer-producible
        "READ_CHAR_FIELD" => tok.len() == 1,
        // outfuncs writes floats with %.*g / shortest-decimal; accept exactly
        // what Rust and C both parse, plus C's spellings of the specials
        "READ_FLOAT_FIELD" => {
            tok.parse::<f64>().is_ok()
                || matches!(tok, "Infinity" | "-Infinity" | "NaN" | "inf" | "-inf" | "nan")
        }
        // A NODE field's value is written by outNode, which emits exactly
        // "<>" (NULL), "{...}" (a node) or "(...)" (a list) — nothing else is
        // writer-producible. Without this rule the fuzzer reaches shapes like
        // `{FROMEXPR :fromlist 2> ...}`, where `2>` is a digit-leading token
        // that C classifies T_Float and happily stores in a node field, while
        // the port expects a list and takes its chartered panic.
        "READ_NODE_FIELD" => matches!(tok, "<>" | "{" | "("),
        // a Bitmapset is written as "(b ...)" or "<>"
        "READ_BITMAPSET_FIELD" => matches!(tok, "<>" | "("),
        // strings (outToken: bare escaped word, `""`, or `<>`) and arrays:
        // shape handled structurally / permissively
        _ => true,
    }
}

/// Faithful port of C `pg_strtok` (read.c) token splitting: whitespace
/// separates, `(){}` are single-character tokens, and a backslash escapes the
/// next byte inside a token. The gate MUST tokenize exactly as C does — the
/// first gate parsed field names with a naive `:`-scan and missed
/// `{GROUPINGSET :kind 0 :content <> K:location -1 }`, where `K:location` is
/// ONE token, not the field name.
fn pg_strtok_all(text: &str) -> Vec<&str> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && matches!(b[i], b' ' | b'\n' | b'\t') {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let start = i;
        if matches!(b[i], b'(' | b')' | b'{' | b'}') {
            i += 1;
        } else {
            while i < b.len()
                && !matches!(b[i], b' ' | b'\n' | b'\t' | b'(' | b')' | b'{' | b'}')
            {
                if b[i] == b'\\' && i + 1 < b.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
        out.push(&text[start..i]);
    }
    out
}

/// Every `{LABEL`-shaped token in the input. Uppercase-or-underscore runs
/// after `{`, exactly the token the C/Rust label dispatch sees.
fn input_labels(text: &str) -> Vec<&str> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < b.len() && (b[j].is_ascii_uppercase() || b[j] == b'_' || b[j].is_ascii_digit())
            {
                j += 1;
            }
            if j > start {
                out.push(&text[start..j]);
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}


/// NONNULL-FIELD CARVES (finding of record, lane p1-nodes).
///
/// The Rust read port asserts these 14 (label, field) pairs are non-NULL
/// (`read_node("f")?.expect(...)`), where C's `READ_NODE_FIELD` accepts NULL
/// without complaint. So for text carrying `:field <>` on one of these,
/// C ACCEPTS and Rust PANICS.
///
/// Why this is a CARVE and not a live defect: every one of these fields is a
/// mandatory child in C's own node contract (`Expr *arg` built by
/// parse-analysis / the rewriter), and the only writer of the catalog columns
/// these crates read is C's own outfuncs over such a node. A NULL there is
/// not reachable from any PG-written text — it requires hand-edited catalog
/// bytes. It IS a robustness delta worth recording: pgrust panics (backend
/// crash class under thread-per-backend) where C would proceed and typically
/// segfault later on the same NULL, so neither side is defensible on
/// hand-corrupted input, and the port is not WORSE.
///
/// Recorded, not silent: the comparator counts these hits separately
/// (NONNULL_CARVES) and `nonnull_carves_match_the_port` asserts the table
/// still equals the port's actual set, so a new expect() in the port either
/// gets a row here or turns the census test red.
pub const NONNULL_FIELD_CARVES: &[(&str, &str)] = &[
    ("RETURNINGEXPR", "retexpr"),
    ("FIELDSELECT", "arg"),
    ("FIELDSTORE", "arg"),
    ("COLLATEEXPR", "arg"),
    ("JOINEXPR", "larg"),
    ("JOINEXPR", "rarg"),
    ("TARGETENTRY", "expr"),
    ("COERCEVIAIO", "arg"),
    ("ARRAYCOERCEEXPR", "arg"),
    ("CONVERTROWTYPEEXPR", "arg"),
    ("PLACEHOLDERVAR", "phexpr"),
    ("RELABELTYPE", "arg"),
    ("COERCETODOMAIN", "arg"),
    ("SUBLINK", "subselect"),
];

/// Executions charged to a NONNULL_FIELD_CARVES hit.
pub static NONNULL_CARVES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// ENUM-DOMAIN CARVES (finding of record, lane p1-nodes).
///
/// The Rust read port VALIDATES 25 node enum fields against their declared
/// value sets and panics on anything else; C's `READ_ENUM_FIELD` casts the
/// integer token blindly, so C ACCEPTS an out-of-domain enum and Rust
/// PANICS. Same reachability argument as the non-null carves: the only
/// writer of these catalog columns is C's own outfuncs over an in-memory
/// node whose enum came from the parser, so out-of-domain values are not
/// reachable from PG-written text. Recorded here rather than waived: the
/// classification is BY PANIC MESSAGE (`readfuncs.c: bad <Enum> <n>`),
/// counted in ENUM_CARVES, and `enum_carves_match_the_port` keeps the list
/// equal to the port's actual validator set.
pub const ENUM_DOMAIN_VALIDATORS: &[&str] = &[
    "NullTestType", "WCOKind", "LockClauseStrength", "LockWaitPolicy",
    "MinMaxOp", "XmlExprOp", "TableFuncType", "ParamKind",
    "JsonConstructorType", "BoolTestType", "CmdType", "QuerySource",
    "XmlOptionType", "RTEKind", "JoinType", "OverridingKind",
    "MergeMatchKind", "CTEMaterialize", "LimitOption", "SetOperation",
    "SubLinkType", "OnConflictAction", "VarReturningType", "CoercionForm",
];

pub static ENUM_CARVES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// VALUE-TOKEN CARVE (charter, documented in the module header).
///
/// C's `nodeTokenType` (read.c) classifies bare tokens into FIVE value-node
/// kinds: `true`/`false` -> Boolean, `"..."` -> String, `b...` -> BitString,
/// digits -> Integer or Float. The Rust read port carries only the two that
/// occur in catalog-stored trees (quoted String, Integer) and panics loudly
/// on the rest — post-parse-analysis expression trees carry literals as
/// `Const` nodes; bare Boolean/Float/BitString value nodes exist only inside
/// raw-grammar `A_Const`, which never reaches these columns.
///
/// The OUT and COPY arms for those tags ARE port surface and ARE compared —
/// by the arm-1 value-node builder, which constructs them programmatically
/// and feeds the rendered text to C. So this carve costs the READ arm only.
pub static VALUE_TOKEN_CARVES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// UNPORTED-SHAPE CARVE (recorded scope gap): shapes the port rejects with an
/// explicit "unported" panic naming the C reader. Exactly one today —
/// `(x ...)`, the XID list — which the READ port does not implement at all
/// (`nodeRead (read.c): xid list unported`), though copyfuncs does dispatch
/// T_XidList. XID lists never appear in the catalog-stored node universe this
/// family is chartered for; recorded here so the gap is visible rather than
/// silently absorbed by another class.
pub static UNPORTED_CARVES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// C `nodeTokenType`'s integer-vs-float decision, verbatim: strip one leading
/// sign, require a digit (or `.digit`), then C's `strtoint` must consume the
/// entire token without ERANGE to call it T_Integer. Anything else numeric is
/// T_Float.
fn c_reads_as_float(tok: &str) -> bool {
    let body = tok.strip_prefix('+').or_else(|| tok.strip_prefix('-')).unwrap_or(tok);
    let b = body.as_bytes();
    let numeric_lead = (!b.is_empty() && b[0].is_ascii_digit())
        || (b.len() > 1 && b[0] == b'.' && b[1].is_ascii_digit());
    if !numeric_lead {
        return false;
    }
    // strtoint == whole token, base 10, i32 range
    match body.parse::<i32>() {
        Ok(_) => false, // T_Integer
        Err(_) => true, // syntax stop or ERANGE -> T_Float
    }
}

/// Classification of a chartered Rust panic.
enum PanicClass {
    /// out-of-charter node label (scoped port) — chartered loud panic
    OutOfCharter,
    /// carved non-null field carrying `<>`
    NonNull,
    /// carved enum field out of its declared domain
    EnumDomain,
    /// bare Boolean/Float/BitString value token (read-set carve)
    ValueToken,
    /// an explicitly unported shape (XID lists)
    Unported,
    /// anything else with every label in scope: a real divergence
    Divergence,
}

fn classify_panic(text: &str, msg: &str, labels: &[&str]) -> PanicClass {
    let port = port_read_labels();
    if !labels.iter().all(|l| port.binary_search(l).is_ok()) {
        return PanicClass::OutOfCharter;
    }
    if msg.contains("unported") {
        return PanicClass::Unported;
    }
    if ENUM_DOMAIN_VALIDATORS
        .iter()
        .any(|e| msg.starts_with(&format!("readfuncs.c: bad {e} ")))
    {
        return PanicClass::EnumDomain;
    }
    // TOKEN-based, not substring-based: `:arg\n\n\n <>` is the same token
    // pair as `:arg <>` and must classify the same (a substring check missed
    // it and reported the carve as a divergence at ~25M local execs).
    {
        let toks = pg_strtok_all(text);
        let hit = toks.windows(2).any(|w| {
            w[1] == "<>"
                && NONNULL_FIELD_CARVES
                    .iter()
                    .any(|(_, field)| w[0] == format!(":{field}"))
        });
        if hit {
            return PanicClass::NonNull;
        }
    }
    // C nodeTokenType (read.c, verbatim rule): a numeric-leading token is
    // T_Integer only if `strtoint` consumes the WHOLE token without ERANGE —
    // otherwise it is T_Float, i.e. a FLOAT value node, which is outside the
    // port's chartered read set. So an over-long or non-integral digit string
    // (`66666666666666666666`, `1.5`) is a VALUE-TOKEN carve, not a
    // divergence, even though the port reports it as a bad integer token.
    for prefix in [
        "readfuncs.c: bad integer token \"",
        "nodeRead (read.c): T_Float value node \"",
    ] {
        if let Some(tok) = msg.strip_prefix(prefix).and_then(|r| r.split('"').next()) {
            if c_reads_as_float(tok) {
                return PanicClass::ValueToken;
            }
        }
    }
    if let Some(tok) = msg
        .strip_prefix("nodeRead (read.c): unhandled token \"")
        .and_then(|r| r.split('"').next())
    {
        // classify by C's OWN rule (nodeTokenType), not by guesswork:
        // T_BitString is `*token == 'b' || *token == 'x'` — BOTH letters (hex
        // bitstrings), which an earlier version of this classifier missed and
        // reported a bare `x` as a divergence.
        let is_bool = tok == "true" || tok == "false";
        let is_bitstring = tok.starts_with('b') || tok.starts_with('x');
        let first = tok.as_bytes().first().copied().unwrap_or(0);
        let numeric_lead = first.is_ascii_digit()
            || ((first == b'-' || first == b'+' || first == b'.')
                && tok.len() > 1);
        if is_bool || is_bitstring || numeric_lead {
            return PanicClass::ValueToken;
        }
    }
    PanicClass::Divergence
}

/// Number of executions that hit the chartered out-of-scope loud-panic arm
/// (visible in unit tests; the fuzz loop just counts).
pub static SCOPE_CARVES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// 54001 (ERRCODE_STATEMENT_TOO_COMPLEX), packed. The GUARD is part of the
/// compared surface (both sides pin 2048kB), but the exact DEPTH at which
/// it fires depends on native frame sizes, which are not a surface — so a
/// one-sided 54001 near the threshold is a documented non-divergence
/// (both-sides-guarded is separately witnessed by the deep-nesting seed
/// test, which drives depth far past both thresholds).
const SQLSTATE_54001: i32 = types_error::make_sqlstate(*b"54001").0;

/// Arm 0: shared text pipeline. Returns true when a full P1..P4 comparison
/// happened (used by the injection sweep + seed tests to prove liveness).
pub fn run_text(input_bytes: &[u8]) -> bool {
    // both sides see the identical NUL-truncated text
    let nul = input_bytes.iter().position(|&b| b == 0).unwrap_or(input_bytes.len());
    let text = match std::str::from_utf8(&input_bytes[..nul]) {
        Ok(t) => t,
        // C would see bytes Rust's &str cannot carry: skip non-UTF-8 —
        // catalog node text is always server-encoding-clean.
        Err(_) => return false,
    };
    // depth pre-bound: the 2048kB guard fires on both sides far below this;
    // keep libFuzzer from burning time on megabyte brace towers
    if text.len() > 1 << 20 {
        return false;
    }
    // C read.c nodeRead's LIST recursion carries NO check_stack_depth in
    // PostgreSQL itself (catalog node text is outfuncs-written, so depth is
    // trusted); only the {LABEL} path is guarded (parseNodeString). A deep
    // bare-paren tower would therefore overflow the REAL C oracle exactly
    // as it overflows a real backend. Harness bound, mirroring the trusted-
    // input posture; the {LABEL} guard itself IS exercised (deep_nesting
    // seed test drives {} depth past both guards -> 54001 both sides).
    {
        let mut depth = 0usize;
        let mut maxd = 0usize;
        for &b in text.as_bytes() {
            match b {
                b'(' => {
                    depth += 1;
                    maxd = maxd.max(depth);
                }
                b')' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        if maxd > 2000 {
            return false;
        }
    }

    // trusted-input contract: malformed node text is UB in C by design
    if !is_well_formed(text) {
        return false;
    }

    let c = c_exec(text.as_bytes());
    let r = rust_exec(text);

    match (c, r) {
        (COut::Err { errcode: ce }, ROut::Err { errcode: re }) => {
            assert_eq!(
                ce, re,
                "ERRCODE DIVERGENCE on {text:?}: C {ce:#x} vs Rust {re:#x}"
            );
            false
        }
        (COut::Err { .. }, ROut::Panic { .. }) => false, // both reject
        // frame-size carve: one-sided stack-guard fire is not a divergence
        (COut::Err { errcode: SQLSTATE_54001 }, ROut::Ok { .. } | ROut::NullNode) => false,
        (COut::Ok { .. }, ROut::Err { errcode: SQLSTATE_54001 }) => false,
        (COut::Err { errcode }, ROut::Ok { .. } | ROut::NullNode) => {
            panic!("ACCEPT DIVERGENCE on {text:?}: C rejected ({errcode:#x}), Rust accepted");
        }
        (COut::Ok { out, .. }, ROut::NullNode) => {
            assert_eq!(out, b"<>", "NULL-NODE DIVERGENCE on {text:?}");
            false
        }
        (COut::Ok { .. }, ROut::Err { errcode }) => {
            panic!("REJECT DIVERGENCE on {text:?}: C accepted, Rust PgError {errcode:#x}");
        }
        (COut::Ok { .. }, ROut::Panic { msg }) => {
            use std::sync::atomic::Ordering::Relaxed;
            let labels = input_labels(text);
            match classify_panic(text, &msg, &labels) {
                PanicClass::OutOfCharter => {
                    SCOPE_CARVES.fetch_add(1, Relaxed);
                }
                PanicClass::NonNull => {
                    NONNULL_CARVES.fetch_add(1, Relaxed);
                }
                PanicClass::EnumDomain => {
                    ENUM_CARVES.fetch_add(1, Relaxed);
                }
                PanicClass::ValueToken => {
                    VALUE_TOKEN_CARVES.fetch_add(1, Relaxed);
                }
                PanicClass::Unported => {
                    UNPORTED_CARVES.fetch_add(1, Relaxed);
                }
                PanicClass::Divergence => panic!(
                    "IN-SCOPE PANIC DIVERGENCE on {text:?}: C accepted, Rust panicked \
                     ({msg:?}), every label {labels:?} is port-dispatched and the panic \
                     matches no recorded carve"
                ),
            }
            false
        }
        (
            COut::Ok { out: co, copy: cc, equal_ok, reread_ok: crr },
            ROut::Ok { out: ro, copy: rc, reread_ok: rrr },
        ) => {
            assert_eq!(
                String::from_utf8_lossy(&co),
                String::from_utf8_lossy(&ro),
                "OUT-TEXT DIVERGENCE on {text:?}"
            );
            assert_eq!(
                String::from_utf8_lossy(&cc),
                String::from_utf8_lossy(&co),
                "C COPY-OUT != C OUT on {text:?}"
            );
            assert_eq!(
                String::from_utf8_lossy(&rc),
                String::from_utf8_lossy(&ro),
                "RUST COPY-OUT != RUST OUT on {text:?}"
            );
            // C's equalfuncs compares float fields with `==`, so a NaN field
            // makes equal(node, copyObject(node)) FALSE in real PostgreSQL
            // too — IEEE semantics, not a copy defect. P1..P3 still hold
            // byte-exactly on NaN, so the copy is verified by text identity.
            assert!(
                equal_ok || text.contains("NaN"),
                "C equal(node, copy) failed on {text:?}"
            );
            assert!(crr, "C round-trip instability on {text:?}");
            assert!(rrr, "Rust round-trip instability on {text:?}");
            true
        }
    }
}

/// Arm 1: value/list nodes built programmatically (Float/Boolean/BitString
/// out+copy arms are unreachable through the scoped read port; C reads the
/// rendered text fine, closing the loop). Bytes drive the shape.
pub fn run_value_nodes(data: &[u8]) -> bool {
    rust_stack_init();
    let cx = mcx::MemoryContext::new("nodesfam_values");
    let m = cx.mcx();
    let Some(node) = build_value_node(m, data) else { return false };

    let out1 = outfuncs::nodeToString(m, node).expect("value out");
    let copy = copyfuncs::copy_object(m, node).expect("value copy");
    let out2 = outfuncs::nodeToString(m, copy).expect("value copy out");
    assert_eq!(out1.as_str(), out2.as_str(), "RUST value copy-out mismatch");

    match c_exec(out1.as_str().as_bytes()) {
        COut::Ok { out, copy, equal_ok, reread_ok } => {
            assert_eq!(
                String::from_utf8_lossy(&out),
                out1.as_str(),
                "VALUE OUT DIVERGENCE (C re-out of rust text)"
            );
            assert_eq!(out, copy, "C value copy-out mismatch");
            assert!(equal_ok && reread_ok, "C value equal/reread failed");
            true
        }
        COut::Err { errcode } => {
            panic!(
                "VALUE READ DIVERGENCE: C rejected rust-rendered {:?} ({errcode:#x})",
                out1.as_str()
            );
        }
    }
}

/// libFuzzer entry: selector byte routes text vs value-builder arm.
pub fn fuzz_entry(data: &[u8]) {
    // one-thread-at-a-time through the C oracles (process-global statics);
    // pg_ndf_init/pg_ndf_exec are holder-checked oracle entries, and this
    // driver is the fuzz TARGET's whole frame stack (task #144 addendum —
    // in-crate tests guard themselves, the target could not).
    let _oracle = crate::oracle_serial();
    let Some((&sel, rest)) = data.split_first() else { return };
    match sel % 4 {
        // text arm gets 3/4 of the budget: it is the read-side surface
        0..=2 => {
            let _live = run_text(rest);
        }
        _ => {
            let _live = run_value_nodes(rest);
        }
    }
}

#[cfg(test)]
#[path = "nodesfam_diff_tests.rs"]
mod tests;
