//! stub:nodes — shared CONSTRUCTED-STATE builder: bounded node trees built
//! from fuzz bytes. Factored verbatim from the p1-nodes harness
//! (fuzz/core/src/nodesfam_diff.rs), which is the migration demo target.
//!
//! BOTH-SIDES DISCIPLINE: the Rust side constructs the tree directly from
//! the bytes; the C-oracle side constructs the SAME tree by reading the
//! Rust tree's nodeToString rendering through the verbatim 18.3 nodeRead —
//! the text plane is the wire contract, and the target's re-out/copy/equal
//! planes compare the C-side structure back against it byte-for-byte. A
//! Rust-side construction difference therefore shows up as an out/reread
//! divergence, never as silent agreement (see stub_controls_tests.rs).
//!
//! CLAMPS (part of the compared-input contract; applied before either side
//! builds):
//!   - tag selector : u8 % 8 (String / Integer / Float / Boolean /
//!                    escaped-String / List / IntList / OidList)
//!   - list nesting : depth < 6, list len % 5
//!   - int/oid lists: len % 6
//!   - strings      : len % 25 (escaped arm % 17), NUL bytes dropped
//!   - Float        : finite f64 rendered via {f:?}, else "1e300" (C
//!                    nodeTokenType classifies by leading char)

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::Node;

/// Intern a &str into the context arena (the copyfuncs str_in shape).
fn intern<'m>(m: Mcx<'m>, s: &str) -> PgResult<&'m str> {
    let v = mcx::slice_in(m, s.as_bytes())?;
    // SAFETY: verbatim copy of a &str
    Ok(unsafe { core::str::from_utf8_unchecked(v.leak()) })
}

/// Bounded value/list node builder over fuzz bytes. Emits every value-node
/// tag the outfuncs port dispatches: String, Integer, Float, Boolean,
/// List (nested), IntList, OidList. (T_BitString is NOT an outfuncs-port
/// tag: catalog-stored expression trees never carry a BitString value node
/// — it exists pre-parse-analysis only, in A_Const under the raw grammar —
/// so it lives in the complement ledger; its copyfuncs arm is exercised by
/// a direct unit test instead.)
pub fn build_value_node<'m>(m: Mcx<'m>, data: &[u8]) -> Option<Node<'m>> {
    let mut it = data.iter().copied();
    build_value_inner(m, &mut it, 0)
}

pub fn build_value_inner<'m>(
    m: Mcx<'m>,
    it: &mut impl Iterator<Item = u8>,
    depth: u32,
) -> Option<Node<'m>> {
    let sel = it.next()?;
    // nodeRead token constraints: strings go through outToken escaping on
    // write, so arbitrary ASCII (NUL-free) is legal; keep them short.
    let mut take_str = |maxlen: usize| -> String {
        let len = (it.next().unwrap_or(0) as usize) % (maxlen + 1);
        let mut s = String::new();
        for _ in 0..len {
            let b = it.next().unwrap_or(b'a');
            if b != 0 {
                s.push(b as char);
            }
        }
        s
    };
    match sel % 8 {
        0 => {
            let sval = take_str(24);
            Node::mk(m, types_nodes::String { sval: intern(m, &sval).ok()? }).ok()
        }
        1 => {
            let mut v = [0u8; 4];
            for b in v.iter_mut() {
                *b = it.next().unwrap_or(0);
            }
            Node::mk(m, types_nodes::Integer { ival: i32::from_le_bytes(v) }).ok()
        }
        2 => {
            // Float carries its literal TEXT (C stores the token string):
            // digits/.eE+- ; C nodeTokenType classifies by leading char, so
            // force a numeric-looking literal.
            let mut v = [0u8; 8];
            for b in v.iter_mut() {
                *b = it.next().unwrap_or(0);
            }
            let f = f64::from_le_bytes(v);
            let lit = if f.is_finite() { format!("{f:?}") } else { "1e300".to_owned() };
            Node::mk(m, types_nodes::Float { fval: intern(m, &lit).ok()? }).ok()
        }
        3 => Node::mk(m, types_nodes::Boolean { boolval: it.next()? & 1 == 1 }).ok(),
        4 => {
            // escaping-heavy strings: outToken's quote/backslash surface
            let raw = take_str(16);
            let mut s = String::new();
            for (i, ch) in raw.chars().enumerate() {
                s.push(match i % 4 {
                    0 => '"',
                    1 => '\\',
                    _ => ch,
                });
            }
            Node::mk(m, types_nodes::String { sval: intern(m, &s).ok()? }).ok()
        }
        5 if depth < 6 => {
            let n = (it.next().unwrap_or(0) as usize) % 5;
            let mut l = types_nodes::NodeList::with_capacity(m, n).ok()?;
            for _ in 0..n {
                l.lappend(m, build_value_inner(m, it, depth + 1)?).ok()?;
            }
            Node::mk_list(m, l).ok()
        }
        6 => {
            let n = (it.next().unwrap_or(0) as usize) % 6;
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                let mut b4 = [0u8; 4];
                for b in b4.iter_mut() {
                    *b = it.next().unwrap_or(0);
                }
                v.push(i32::from_le_bytes(b4));
            }
            Node::mk_int_list(m, types_nodes::list::IntList::from_slice(m, &v).ok()?).ok()
        }
        _ => {
            let n = (it.next().unwrap_or(0) as usize) % 6;
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                let mut b4 = [0u8; 4];
                for b in b4.iter_mut() {
                    *b = it.next().unwrap_or(0);
                }
                v.push(u32::from_le_bytes(b4)); // types_core::Oid = u32
            }
            Node::mk_oid_list(m, types_nodes::list::OidList::from_slice(m, &v).ok()?).ok()
        }
    }
}

