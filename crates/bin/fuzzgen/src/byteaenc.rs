//! bytea + encode/decode + encoding-convert drain module (BYTEAENC): the
//! Track-B SQL-drainable surface of varlena.c (bytea funcs), encode.c
//! (base64/hex/escape codecs) and the text-encoding conversion entries in
//! mbutils.c that the adtmisc `vlbytea`/`byteax` long-tail and the mbconv
//! matrix left shallow or untouched.
//!
//! What this module drills that the existing modules do not:
//!   - the `bytea_output` GUC (hex vs escape) rendering path, in a
//!     self-contained SET escape .. RESET bracket (reverse-RESET always
//!     emitted; bytea_output is NOT in the C-parity/datetime pins, and a
//!     specific RESET never un-pins them);
//!   - text<->bytea casts (textin/byteaout round trips) and the
//!     int2/int4/int8<->bytea casts;
//!   - the full user-facing comparison-operator matrix (= <> < <= > >=,
//!     byteacmp, bytea_larger/smaller, min/max) under a TOTAL ORDER BY over
//!     a fixed VALUES set — bytea compare is raw unsigned memcmp with
//!     shorter-is-less, EXACT on both engines (any divergence is a real
//!     HIGH finding; there is no ruled tie-order surface here). NOTE: bytea
//!     has NO `~<~`/`~>~` pattern-ordering operators (verified against
//!     REL_18_3 pg_operator + varlena/builtins.rs); only text/bpchar/name
//!     carry those, so they are deliberately absent.
//!   - systematic encode/decode round trips across all three formats with
//!     matched malformed-input error arms (odd/invalid hex, bad base64
//!     padding/alphabet, truncated/invalid escape);
//!   - get_byte/set_byte/get_bit/set_bit with deterministic in-range edges
//!     AND the out-of-range / bad-new-bit error arms (22003);
//!   - a focused convert/convert_from/convert_to arm over the safe server
//!     encodings (UTF8/LATIN1/SQL_ASCII) plus the invalid-byte-sequence
//!     identity (22021) — deliberately light, complementing the verified
//!     128-pair mbconv matrix rather than replicating it.
//!
//! Comparison law: every scalar result is cast ::text (bytea outputs too)
//! so the differ compares byte-identical text; the cmp family uses a total
//! ORDER BY for a deterministic row order. Errors are part of the surface:
//! malformed-input / out-of-range arms are emitted deliberately and matched
//! on SQLSTATE by the differ (diff::classify). Every statement is a
//! self-contained one-statement group except the bytea_output bracket,
//! which is a fixed SET/probe/RESET sequence of independent statements.
//! Determinism: every payload is a fixed literal; no engine-computed input
//! ever feeds a later statement.

use crate::stmt::{Gen, StmtKind};

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

/// Hex bodies for `'\xBODY'::bytea` literals, spanning empty / single /
/// high-bit / multi-byte payloads (the memcmp + length-prefix regimes).
const HEX_BODIES: &[&str] = &[
    "", "00", "01", "ff", "00ff", "7f80", "deadbeef", "0102030405",
    "ff00ff00", "c0ffee00", "000102fdfeff", "8081828384858687",
];

/// A `'\xBODY'::bytea` literal drawn from the hex pool.
fn bva(g: &mut Gen) -> String {
    format!("'\\x{}'::bytea", pick_str(g, HEX_BODIES))
}

const SHAPES: &[&str] = &[
    "byteaenc:enc",
    "byteaenc:encerr",
    "byteaenc:getset",
    "byteaenc:ops",
    "byteaenc:cmp",
    "byteaenc:cast",
    "byteaenc:conv",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_byteaenc_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc");
    match g.weights.pick(g.rng, SHAPES) {
        "byteaenc:enc" => gen_enc(g),
        "byteaenc:encerr" => gen_encerr(g),
        "byteaenc:getset" => gen_getset(g),
        "byteaenc:ops" => gen_ops(g),
        "byteaenc:cmp" => gen_cmp(g),
        "byteaenc:cast" => gen_cast(g),
        _ => gen_conv(g),
    }
}

// ------------------------------------------------------------------ enc ----

/// encode/decode across hex/base64/escape, including round trips
/// (encode(decode(x))=x and decode(encode(b))=b) — encode.c pg_*_encode /
/// pg_*_decode success paths incl. the base64 76-column line-wrap.
fn gen_enc(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc:enc");
    let shape = g.weights.pick(
        g.rng,
        &["byteaenc:enc:hex", "byteaenc:enc:b64", "byteaenc:enc:esc", "byteaenc:enc:rt"],
    );
    g.fire(shape);
    let sql = match shape {
        "byteaenc:enc:hex" => {
            let b = bva(g);
            let h = pick_str(g, &["", "00", "12ab", "deadBEEF", "00ff00", "0102030405060708"]);
            format!("SELECT encode({b}, 'hex'), decode('{h}', 'hex')::text;")
        }
        "byteaenc:enc:b64" => {
            // wide payload exercises the 76-col wrap in the encoder.
            let b = pick_str(
                g,
                &[
                    "'\\x'::bytea",
                    "'\\x00'::bytea",
                    "'\\xdeadbeef'::bytea",
                    "('\\x' || repeat('ab', 40))::bytea",
                    "('\\x' || repeat('00ff', 30))::bytea",
                ],
            );
            let s = pick_str(g, &["", "AA==", "3q2+7w==", "QUJDREVGRw==", "aGVsbG8gd29ybGQ="]);
            format!("SELECT encode({b}, 'base64'), decode('{s}', 'base64')::text;")
        }
        "byteaenc:enc:esc" => {
            let b = bva(g);
            // valid escape-form decode inputs: octal escapes + printable +
            // doubled-backslash (a literal backslash byte).
            let s = pick_str(g, &["", "abc", "\\001\\002\\003", "a\\177z", "x\\\\y", "\\000end"]);
            format!("SELECT encode({b}, 'escape'), decode('{s}', 'escape')::text;")
        }
        // round trips in both directions across all three formats.
        _ => {
            let b = bva(g);
            let fmt = pick_str(g, &["hex", "base64", "escape"]);
            format!(
                "SELECT decode(encode({b}, '{fmt}'), '{fmt}')::text = {b}::text, \
                 encode(decode(encode({b}, '{fmt}'), '{fmt}'), '{fmt}') = encode({b}, '{fmt}');"
            )
        }
    };
    vec![raw(sql)]
}

// --------------------------------------------------------------- encerr ----

/// Matched malformed-input error arms for each decode format (SQLSTATE
/// compared by the differ): odd-length / invalid-digit hex, bad base64
/// padding+alphabet, truncated/invalid escape, and the unknown-format arm.
fn gen_encerr(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc:encerr");
    let shape = g.weights.pick(
        g.rng,
        &["byteaenc:err:hex", "byteaenc:err:b64", "byteaenc:err:esc", "byteaenc:err:fmt"],
    );
    g.fire(shape);
    let sql = match shape {
        // 22P03/22023-class: odd length, invalid hex digit, embedded space.
        "byteaenc:err:hex" => {
            let s = pick_str(g, &["f", "abc", "0g", "zz", "12 34", "12x4", "-1"]);
            format!("SELECT decode('{s}', 'hex')::text;")
        }
        // invalid base64: bad padding, non-alphabet char, wrong length.
        "byteaenc:err:b64" => {
            let s = pick_str(g, &["A", "AB", "====", "####", "3q2+7w=", "a==b", "@@@@"]);
            format!("SELECT decode('{s}', 'base64')::text;")
        }
        // invalid escape: trailing backslash, non-octal after backslash,
        // out-of-range/short octal.
        "byteaenc:err:esc" => {
            let s = pick_str(g, &["trail\\", "ab\\9", "\\x", "\\40\\9", "\\77\\", "bad\\g"]);
            format!("SELECT decode('{s}', 'escape')::text;")
        }
        // unknown codec name (matched 22023).
        _ => {
            let f = pick_str(g, &["bogus", "hex2", "BASE64", "", "uu"]);
            format!("SELECT decode('00', '{f}')::text;")
        }
    };
    vec![raw(sql)]
}

// -------------------------------------------------------------- getset ----

/// get_byte/set_byte (byte index) and get_bit/set_bit (bit index) with
/// deterministic in-range edges and the out-of-range / bad-new-bit (22003)
/// error arms, over the fixed 4-byte '\xdeadbeef' (len 4, bits 0..31).
fn gen_getset(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc:getset");
    let shape =
        g.weights.pick(g.rng, &["byteaenc:gs:byte", "byteaenc:gs:bit", "byteaenc:gs:err"]);
    g.fire(shape);
    let sql = match shape {
        "byteaenc:gs:byte" => {
            let i = g.rng.below(4); // 0..3 in range
            let v = pick_str(g, &["0", "1", "127", "128", "255"]);
            format!(
                "SELECT get_byte('\\xdeadbeef'::bytea, {i}), \
                 set_byte('\\xdeadbeef'::bytea, {i}, {v})::text;"
            )
        }
        "byteaenc:gs:bit" => {
            let i = g.rng.below(32); // 0..31 in range
            let v = g.rng.below(2);
            format!(
                "SELECT get_bit('\\xdeadbeef'::bytea, {i}), \
                 set_bit('\\xdeadbeef'::bytea, {i}, {v})::text;"
            )
        }
        // out-of-range index (byte 4 / bit 32) and bad new-bit value (>1).
        _ => {
            let e = pick_str(
                g,
                &[
                    "SELECT get_byte('\\xdeadbeef'::bytea, 4);",        // 22003
                    "SELECT get_byte('\\xdeadbeef'::bytea, -1);",       // 22003
                    "SELECT set_byte('\\xdeadbeef'::bytea, 4, 0)::text;", // 22003
                    "SELECT get_bit('\\xdeadbeef'::bytea, 32);",        // 22003
                    "SELECT set_bit('\\xdeadbeef'::bytea, 32, 1)::text;", // 22003
                    "SELECT set_bit('\\xdeadbeef'::bytea, 0, 2)::text;",  // 22003 bad bit
                    "SELECT get_byte(''::bytea, 0);",                    // 22003 empty
                ],
            );
            e.to_string()
        }
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------------ ops ----

/// The bytea function/operator surface: `||`, position, substring/substr
/// (from/for with zero/negative/huge bounds), overlay, length/octet_length/
/// bit_count/reverse, and ltrim/rtrim/btrim(bytea, bytea).
fn gen_ops(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc:ops");
    let shape = g.weights.pick(
        g.rng,
        &[
            "byteaenc:op:cat",
            "byteaenc:op:sub",
            "byteaenc:op:pos",
            "byteaenc:op:trim",
            "byteaenc:op:len",
            "byteaenc:op:overlay",
        ],
    );
    g.fire(shape);
    let sql = match shape {
        "byteaenc:op:cat" => {
            let a = bva(g);
            let b = bva(g);
            format!("SELECT ({a} || {b})::text, ({a} || ''::bytea)::text;")
        }
        "byteaenc:op:sub" => {
            let a = bva(g);
            let from = g.rng.range_i64(-2, 6);
            let forr = g.rng.range_i64(0, 6);
            format!(
                "SELECT substr({a}, {from})::text, substring({a} from {from} for {forr})::text, \
                 substr({a}, {from}, {forr})::text;"
            )
        }
        "byteaenc:op:pos" => {
            let a = bva(g);
            let b = pick_str(g, &["'\\x'::bytea", "'\\xad'::bytea", "'\\xff'::bytea", "'\\xdead'::bytea"]);
            format!("SELECT position({b} in {a}), position({a} in {a});")
        }
        "byteaenc:op:trim" => {
            let a = pick_str(
                g,
                &["'\\x0011220000'", "'\\x000000'", "'\\xaabbaa'", "'\\x'", "'\\xffaaff'"],
            );
            let set = pick_str(g, &["'\\x00'", "'\\xaa'", "'\\x00ff'", "'\\x'", "'\\xff'"]);
            let f = pick_str(g, &["ltrim", "rtrim", "btrim"]);
            format!("SELECT {f}({a}::bytea, {set}::bytea)::text;")
        }
        "byteaenc:op:len" => {
            let a = bva(g);
            format!(
                "SELECT length({a}), octet_length({a}), bit_count({a}), reverse({a})::text;"
            )
        }
        _ => {
            let a = bva(g);
            let b = pick_str(g, &["'\\x00'::bytea", "'\\xffff'::bytea", "'\\x'::bytea"]);
            let from = g.rng.range_i64(1, 6);
            let forr = g.rng.range_i64(0, 5);
            format!(
                "SELECT overlay({a} placing {b} from {from})::text, \
                 overlay({a} placing {b} from {from} for {forr})::text;"
            )
        }
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------------ cmp ----

/// bytea comparison: the six operators + byteacmp sign + bytea_larger/
/// smaller over literal pairs, and a total-ORDER-BY sort over a fixed
/// VALUES set (raw unsigned memcmp, shorter-is-less; EXACT on both sides).
fn gen_cmp(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc:cmp");
    let shape =
        g.weights.pick(g.rng, &["byteaenc:cmp:ops", "byteaenc:cmp:order", "byteaenc:cmp:fn"]);
    g.fire(shape);
    let sql = match shape {
        "byteaenc:cmp:ops" => {
            let a = bva(g);
            let b = bva(g);
            format!(
                "SELECT {a} < {b}, {a} <= {b}, {a} = {b}, {a} >= {b}, {a} > {b}, {a} <> {b};"
            )
        }
        // Total ORDER BY over a fixed set: length-prefix + high-bit bytes
        // stress the shorter-is-less and unsigned-byte ordering rules.
        "byteaenc:cmp:order" => "SELECT v::text FROM (VALUES \
             ('\\x'::bytea), ('\\x00'), ('\\x0000'), ('\\x01'), ('\\x0100'), \
             ('\\x7f'), ('\\x80'), ('\\xff'), ('\\xff00'), ('\\xffff'), \
             ('\\xdead'), ('\\xdeadbeef')) t(v) ORDER BY v;"
            .to_string(),
        _ => {
            let a = bva(g);
            let b = bva(g);
            format!(
                "SELECT byteacmp({a}, {b}), \
                 sign(byteacmp({a}, {b})), \
                 bytea_larger({a}, {b})::text, bytea_smaller({a}, {b})::text;"
            )
        }
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------------- cast ----

/// text<->bytea and int<->bytea casts, plus the bytea_output (hex/escape)
/// rendering bracket. The bracket ALWAYS reverse-RESETs bytea_output (it is
/// not in the session pins, and a specific RESET never un-pins them).
fn gen_cast(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc:cast");
    let shape =
        g.weights.pick(g.rng, &["byteaenc:ca:text", "byteaenc:ca:int", "byteaenc:ca:guc"]);
    g.fire(shape);
    match shape {
        // text -> bytea (raw bytes of the text) and bytea -> text.
        "byteaenc:ca:text" => {
            let t = pick_str(g, &["'abc'", "''", "'hi there'", "'~!@#'", "'0'"]);
            let b = bva(g);
            vec![raw(format!(
                "SELECT ({t}::text::bytea)::text, ({b}::text)::bytea::text = {b}::text;"
            ))]
        }
        // int2/int4/int8 <-> bytea (fixed-width big-endian images).
        "byteaenc:ca:int" => {
            let n2 = g.rng.below(30000);
            let n4 = g.rng.below(2000000);
            vec![raw(format!(
                "SELECT ({n2}::int2)::bytea::text, ({n4}::int4)::bytea::text, \
                 ('\\x0102'::bytea)::int2, ('\\x00010203'::bytea)::int4, \
                 ('\\x0000000218711a00'::bytea)::int8;"
            ))]
        }
        // bytea_output GUC hex vs escape rendering, self-contained bracket.
        _ => {
            let b = bva(g);
            vec![
                raw("SET bytea_output = escape;".to_string()),
                raw(format!("SELECT {b}, decode('deadbeef', 'hex');")),
                raw("SET bytea_output = hex;".to_string()),
                raw(format!("SELECT {b}, decode('deadbeef', 'hex');")),
                raw("RESET bytea_output;".to_string()),
            ]
        }
    }
}

// ----------------------------------------------------------------- conv ----

/// Focused text-encoding conversion arm: convert/convert_from/convert_to
/// over the safe server encodings (UTF8/LATIN1/SQL_ASCII — ASCII-only
/// payloads are representable in all three) plus the invalid-byte-sequence
/// identity (22021). Deliberately light — the verified 128-pair matrix
/// lives in crate::mbconv; this only pins the round-trip and error identity
/// on the always-available encodings.
fn gen_conv(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("byteaenc:conv");
    let shape = g.weights.pick(g.rng, &["byteaenc:cv:roundtrip", "byteaenc:cv:bad"]);
    g.fire(shape);
    let sql = match shape {
        "byteaenc:cv:roundtrip" => {
            let enc = pick_str(g, &["UTF8", "LATIN1", "SQL_ASCII"]);
            let txt = pick_str(g, &["'hello'", "'ABC123'", "''", "'a-z_0.9'"]);
            format!(
                "SELECT convert_to({txt}, '{enc}')::text, \
                 convert_from(convert_to({txt}, '{enc}'), '{enc}'), \
                 convert('\\x414243'::bytea, 'SQL_ASCII', '{enc}')::text;"
            )
        }
        // invalid byte sequence for the target/source encoding (22021).
        _ => {
            let e = pick_str(
                g,
                &[
                    "SELECT convert_from('\\xff'::bytea, 'UTF8');",         // 22021
                    "SELECT convert_from('\\xc0'::bytea, 'UTF8');",         // 22021 truncated
                    "SELECT convert_from('\\xeda080'::bytea, 'UTF8');",     // 22021 (surrogate)
                    "SELECT convert('\\xff'::bytea, 'UTF8', 'LATIN1')::text;", // 22021 src invalid
                ],
            );
            e.to_string()
        }
    };
    vec![raw(sql)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every emitted statement is single-line, terminated, paren-balanced —
    /// the same invariants the stmt-registry test enforces, checked here
    /// with a byteaenc-heavy weight so all shapes fire.
    #[test]
    fn all_shapes_well_formed() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xB17EA);
        let mut saw = std::collections::HashSet::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_byteaenc_module(&mut g);
            assert!(!stmts.is_empty());
            for s in &stmts {
                let sql = s.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
            for p in prods {
                saw.insert(p);
            }
        }
        // All seven top shapes fired.
        for shape in SHAPES {
            assert!(saw.contains(*shape), "shape never fired: {shape}");
        }
    }

    /// The bytea_output bracket always ends with a RESET (reverse-RESET
    /// discipline) whenever a SET bytea_output was emitted.
    #[test]
    fn guc_bracket_reverse_resets() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        // Force the cast module, guc sub-shape.
        let w = WeightTable::parse(
            "byteaenc:enc=0,byteaenc:encerr=0,byteaenc:getset=0,byteaenc:ops=0,\
             byteaenc:cmp=0,byteaenc:conv=0,byteaenc:ca:text=0,byteaenc:ca:int=0",
        )
        .unwrap();
        let mut rng = Rng::new(7);
        let mut saw_bracket = false;
        for _ in 0..200 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_byteaenc_module(&mut g);
            let texts: Vec<String> = stmts.iter().map(|s| s.to_sql()).collect();
            if texts.iter().any(|t| t.starts_with("SET bytea_output")) {
                saw_bracket = true;
                assert!(
                    texts.iter().any(|t| t == "RESET bytea_output;"),
                    "SET bytea_output without reverse-RESET: {texts:?}"
                );
            }
        }
        assert!(saw_bracket, "guc bracket never fired under forced weights");
    }
}
