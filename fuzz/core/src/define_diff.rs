//! define_diff: differential fuzz driver — shipped Rust `define` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_define_io.c). Crate under test: crates/backend/commands/define.
//!
//! Comparison planes (float_in_diff conventions): value bytes, error-verdict,
//! and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][argkind][flags][ival:4LE][strings, 0xFF-separated]
//!   selector % 2 picks the arm:
//!     0 defGetString  — argkind % 6 ∈ {none, Integer, Float, Boolean,
//!       String, TypeName}; strings = defname, s, name0..name3.
//!     1 defGetBoolean — argkind % 5 (TypeName excluded; see SKIPPED).
//!   flags: bit0 = pct_type, bits1-2 = arrayBounds length (0..3),
//!   bits3-4 = nnames-1 (1..4). Strings must be UTF-8, NUL-free, <=256 bytes
//!   each (C-string / &str common domain; the driver skips otherwise).
//!
//! FC plane: none — this crate ships no fc_* wrappers (defGetString /
//! defGetBoolean are backend-internal DefElem decoders, not SQL callables).
//!
//! SKIPPED (unported-arm carve, recorded as exception rows; the shipped Rust
//! panics with an "unported (define lane)" message on each):
//!   - defGetString on T_List / T_A_Star args (C: NameListToString /
//!     pstrdup("*")) — lib.rs panic arm.
//!   - defGetString on a TypeName with empty `names` (C: format_type_be
//!     lookup, catalog-dependent — not a pure surface) — lib.rs panic arm.
//!   - defGetBoolean on non-scalar args (C routes them through defGetString;
//!     the shipped Rust's no-alloc split panics on TypeName/List/A_Star) —
//!     lib.rs panic arm. Boolean-valued DDL options reach defGetBoolean as
//!     String/Integer/Boolean/Float nodes via opt_boolean_or_string / def_arg.
//!   The C oracle stubs the corresponding helpers with loud abort()s, so any
//!   domain drift is caught at once.

use types_error::{PgError, ERRCODE_SYNTAX_ERROR};
use types_nodes::parsenodes::DefElem;
use types_nodes::rawnodes::TypeName;
use types_nodes::list::NodeList;
use types_nodes::Node;

extern "C" {
    fn pg_diff_defGetString(
        argkind: i32,
        defname: *const u8,
        ival: i32,
        s: *const u8,
        names: *const *const u8,
        nnames: i32,
        pct_type: i32,
        nbounds: i32,
        out: *mut u8,
        outcap: i32,
    ) -> i32;
    fn pg_diff_defGetBoolean(
        argkind: i32,
        defname: *const u8,
        ival: i32,
        s: *const u8,
        bool_out: *mut i32,
    ) -> i32;
}

/// Oracle error classes (must match the defines in csrc/pg_define_io.c).
const C_ERR_SYNTAX: i32 = 1; /* 42601 */

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_SYNTAX_ERROR {
        C_ERR_SYNTAX
    } else {
        99
    }
}

const MAX_STR: usize = 256;

/// Decoded fixture description shared by both arms. `names` = how many of
/// the caller's string slots (indices 2..) are TypeName name components.
struct Fixture {
    argkind: u8,
    pct_type: bool,
    nbounds: usize,
    nnames: usize,
    ival: i32,
}

/// Split `bytes` on 0xFF into up to `n` UTF-8, NUL-free, <=MAX_STR strings.
/// Missing trailing fields decode as "".
fn decode_strings<'a>(bytes: &'a [u8], out: &mut [&'a str]) -> bool {
    let mut it = bytes.split(|&b| b == 0xFF);
    for slot in out.iter_mut() {
        let piece = it.next().unwrap_or(&[]);
        if piece.len() > MAX_STR || piece.contains(&0) {
            return false;
        }
        let Ok(s) = core::str::from_utf8(piece) else {
            return false;
        };
        *slot = s;
    }
    true
}

fn decode<'a>(payload: &'a [u8], strs: &mut [&'a str; 6], boolean_arm: bool) -> Option<Fixture> {
    if payload.len() < 6 {
        return None;
    }
    let argkind = if boolean_arm { payload[0] % 5 } else { payload[0] % 6 };
    let flags = payload[1];
    let ival = i32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);
    if !decode_strings(&payload[6..], &mut strs[..]) {
        return None;
    }
    Some(Fixture {
        argkind,
        pct_type: flags & 1 != 0,
        nbounds: ((flags >> 1) & 0x3) as usize,
        nnames: ((flags >> 3) & 0x3) as usize + 1,
        ival,
    })
}

/// Build the Rust-side arg Node for the fixture (mirrors the C
/// pg_define_build_arg fixture constructor; environment, not computation).
fn build_arg<'mcx>(
    m: mcx::Mcx<'mcx>,
    fx: &Fixture,
    s: &str,
    names: &[&str],
) -> Option<Option<Node<'mcx>>> {
    let node = match fx.argkind {
        0 => return Some(None),
        1 => Node::mk_integer(m, fx.ival).ok()?,
        2 => Node::mk_float(m, str_in(m, s)?).ok()?,
        3 => Node::mk_boolean(m, fx.ival & 1 != 0).ok()?,
        4 => Node::mk_string(m, str_in(m, s)?).ok()?,
        5 => {
            let mut names_l = NodeList::with_capacity(m, names.len()).ok()?;
            for n in names {
                names_l.lappend(m, Node::mk_string(m, str_in(m, n)?).ok()?).ok()?;
            }
            let mut bounds = NodeList::with_capacity(m, fx.nbounds).ok()?;
            for _ in 0..fx.nbounds {
                // PG uses Integer(-1) for unbounded dimensions; contents are
                // never read by either side, only the list's NIL-ness/len.
                bounds.lappend(m, Node::mk_integer(m, -1).ok()?).ok()?;
            }
            Node::mk(
                m,
                TypeName {
                    names: names_l,
                    pct_type: fx.pct_type,
                    arrayBounds: bounds,
                    ..Default::default()
                },
            )
            .ok()?
        }
        _ => unreachable!(),
    };
    Some(Some(node))
}

/// Copy a &str into the mcx arena (fixture strings need 'mcx lifetime).
fn str_in<'mcx>(m: mcx::Mcx<'mcx>, s: &str) -> Option<&'mcx str> {
    let bytes = mcx::slice_borrow_in(m, s.as_bytes()).ok()?;
    // SAFETY: byte-for-byte copy of a &str.
    Some(unsafe { core::str::from_utf8_unchecked(bytes) })
}

/// NUL-terminated C image of a Rust str (validated NUL-free by decode).
fn czstr(s: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn define_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 2 {
        0 => defGetString_diff(payload),
        _ => defGetBoolean_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: defGetString (C source: define.c).
// ---------------------------------------------------------------------------

#[allow(non_snake_case)]
fn defGetString_diff(payload: &[u8]) {
    let mut strs: [&str; 6] = [""; 6];
    let Some(fx) = decode(payload, &mut strs, false) else {
        return;
    };
    let (defname, s) = (strs[0], strs[1]);
    let names = &strs[2..2 + fx.nnames];

    // C oracle.
    let defname_c = czstr(defname);
    let s_c = czstr(s);
    let names_c: Vec<Vec<u8>> = names.iter().map(|n| czstr(n)).collect();
    let name_ptrs: Vec<*const u8> = names_c.iter().map(|v| v.as_ptr()).collect();
    let mut out = [0u8; 2048];
    let cst = unsafe {
        pg_diff_defGetString(
            fx.argkind as i32,
            defname_c.as_ptr(),
            fx.ival,
            s_c.as_ptr(),
            name_ptrs.as_ptr(),
            names.len() as i32,
            fx.pct_type as i32,
            fx.nbounds as i32,
            out.as_mut_ptr(),
            out.len() as i32,
        )
    };

    // Shipped Rust core.
    let cx = mcx::MemoryContext::new("define_fuzz");
    let m = cx.mcx();
    let Some(arg) = build_arg(m, &fx, s, names) else {
        return; // mcx alloc failure: no C counterpart, skip exec
    };
    let def = DefElem {
        defnamespace: None,
        defname: Some(str_in(m, defname).unwrap()),
        arg,
        defaction: Default::default(),
        location: Default::default(),
    };
    let rres = define::defGetString(m, &def);

    // Planes.
    match (cst, rres) {
        (0, Ok(rs)) => {
            let clen = out.iter().position(|&b| b == 0).unwrap();
            assert_eq!(
                rs.as_bytes(),
                &out[..clen],
                "defGetString value plane diverged (argkind={})",
                fx.argkind
            );
        }
        (c, Err(e)) if c != 0 => {
            assert_eq!(
                rust_err_class(&e),
                c,
                "defGetString sqlstate plane diverged (argkind={})",
                fx.argkind
            );
        }
        (c, r) => panic!(
            "defGetString verdict plane diverged (argkind={}): C={} Rust ok={}",
            fx.argkind,
            c,
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm: defGetBoolean (C source: define.c).
// ---------------------------------------------------------------------------

#[allow(non_snake_case)]
fn defGetBoolean_diff(payload: &[u8]) {
    let mut strs: [&str; 6] = [""; 6];
    let Some(fx) = decode(payload, &mut strs, true) else {
        return;
    };
    let (defname, s) = (strs[0], strs[1]);

    // C oracle.
    let defname_c = czstr(defname);
    let s_c = czstr(s);
    let mut cbool: i32 = -1;
    let cst = unsafe {
        pg_diff_defGetBoolean(
            fx.argkind as i32,
            defname_c.as_ptr(),
            fx.ival,
            s_c.as_ptr(),
            &mut cbool,
        )
    };

    // Shipped Rust core.
    let cx = mcx::MemoryContext::new("define_fuzz");
    let m = cx.mcx();
    let Some(arg) = build_arg(m, &fx, s, &[]) else {
        return;
    };
    let def = DefElem {
        defnamespace: None,
        defname: Some(str_in(m, defname).unwrap()),
        arg,
        defaction: Default::default(),
        location: Default::default(),
    };
    let rres = define::defGetBoolean(&def);

    match (cst, rres) {
        (0, Ok(rb)) => {
            assert_eq!(
                rb,
                cbool != 0,
                "defGetBoolean value plane diverged (argkind={})",
                fx.argkind
            );
        }
        (c, Err(e)) if c != 0 => {
            assert_eq!(
                rust_err_class(&e),
                c,
                "defGetBoolean sqlstate plane diverged (argkind={})",
                fx.argkind
            );
        }
        (c, r) => panic!(
            "defGetBoolean verdict plane diverged (argkind={}): C={} Rust ok={}",
            fx.argkind,
            c,
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(sel: u8, argkind: u8, flags: u8, ival: i32, strs: &[&str]) -> Vec<u8> {
        let mut v = vec![sel, argkind, flags];
        v.extend_from_slice(&ival.to_le_bytes());
        for (i, s) in strs.iter().enumerate() {
            if i > 0 {
                v.push(0xFF);
            }
            v.extend_from_slice(s.as_bytes());
        }
        v
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/define_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/define_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                define_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Per-arm smoke: ok + error shapes for both arms, every argkind.
    #[test]
    fn arms_smoke() {
        // defGetString: none (error), Integer, Float, Boolean, String,
        // TypeName (qualified, %TYPE, bounds).
        define_diff(&mk(0, 0, 0, 0, &["opt"]));
        define_diff(&mk(0, 1, 0, -2147483648, &["opt"]));
        define_diff(&mk(0, 1, 0, 2147483647, &["opt"]));
        define_diff(&mk(0, 2, 0, 0, &["opt", "3.14"]));
        define_diff(&mk(0, 3, 0, 1, &["opt"]));
        define_diff(&mk(0, 3, 0, 0, &["opt"]));
        define_diff(&mk(0, 4, 0, 0, &["opt", "hello world"]));
        define_diff(&mk(0, 5, 0, 0, &["opt", "", "pg_catalog"]));
        define_diff(&mk(0, 5, 0b0000_1000, 0, &["opt", "", "pg_catalog", "int4"]));
        define_diff(&mk(0, 5, 0b0000_0001, 0, &["opt", "", "tab", "col"]));
        define_diff(&mk(0, 5, 0b0000_0010, 0, &["opt", "", "int4"])); // 1 bound
        define_diff(&mk(0, 5, 0b0000_0100, 0, &["opt", "", "int4"])); // 2 bounds
        // defGetBoolean: none=true, 0/1/other ints, true/on/false/off/other
        // strings, Boolean, Float.
        define_diff(&mk(1, 0, 0, 0, &["opt"]));
        define_diff(&mk(1, 1, 0, 0, &["opt"]));
        define_diff(&mk(1, 1, 0, 1, &["opt"]));
        define_diff(&mk(1, 1, 0, 2, &["opt"]));
        define_diff(&mk(1, 2, 0, 0, &["opt", "1.5"]));
        define_diff(&mk(1, 3, 0, 1, &["opt"]));
        define_diff(&mk(1, 4, 0, 0, &["opt", "TrUe"]));
        define_diff(&mk(1, 4, 0, 0, &["opt", "ON"]));
        define_diff(&mk(1, 4, 0, 0, &["opt", "false"]));
        define_diff(&mk(1, 4, 0, 0, &["opt", "oFf"]));
        define_diff(&mk(1, 4, 0, 0, &["opt", "maybe"]));
    }
}
