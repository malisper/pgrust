//! BITSTRING drain module: the bit / bit varying (varbit.c) function and
//! operator surface, complementing the TYPEIO lane (which touched the raw
//! `>>`/`<<`/`#`/`~`/`&`/`|`/cmp operator arms) by draining the *functions*
//! and the *edge* branches TYPEIO left hollow — hex `X'…'` input, the empty
//! bit string everywhere, the int4/int8 sign-fill cast widths and their
//! exact overflow boundaries, unaligned concatenation, byte-aligned and
//! over-length shifts, substring/overlay length/overflow arms, and every
//! error-identity arm with its exact SQLSTATE.
//!
//! Everything is a single `StmtKind::Raw` over deterministic literal inputs
//! (the expr/rich productions already carry the column-borne surface, and
//! the adtmisc `adtm:bit*` families carry the random-literal happy path), so
//! the AST scoping checker has nothing to check and the differ's byte-exact
//! (scalar `::text`) / multiset (inline-VALUES ordered) compare covers it.
//!
//! Validity / compare-safety disciplines:
//!   - every scalar output is cast `::text` (or is already int / boolean):
//!     bit values compare byte-identical on both engines and the text cast
//!     pins both the value and the result type oid on the wire;
//!   - set-returning arms run over inline `VALUES` (no persistent table, so
//!     no interaction with the dml pk-persistence invariant) and always
//!     carry a *total* `ORDER BY … ::text` so the row order is determined;
//!   - deliberate error fuel (invalid binary/hex digit 22P02, fixed-length
//!     mismatch 22026, varying too-long 22001, different-size logic 22026,
//!     negative substring 2201B, subscript out-of-range 2202E, new-bit not
//!     0/1 22023, int overflow 22003, typmod length < 1 22023) rides one
//!     `bitstr:ok`/`bitstr:err` weight pair, biased toward the ok arm per
//!     the findings-budget rule; the differ compares error identity
//!     (SQLSTATE + message) exactly like a value;
//!   - no nondeterministic function is ever emitted.

use crate::stmt::{Gen, StmtKind};

/// Top-level statement shapes (weighted pick; all registered in
/// `weights::PROD_WEIGHTS`).
const SHAPES: &[&str] = &[
    "bitstr:io",       // bit_in / varbit_in / bits_out: binary + hex + empty
    "bitstr:typmod",   // bit(n) truncate/pad + length-mismatch; varbit(n)
    "bitstr:cast",     // bit <-> int4/int8 sign-fill widths + overflow
    "bitstr:cat",      // concatenation || incl. unaligned first operand
    "bitstr:logic",    // & | # ~ incl. different-size error
    "bitstr:shift",    // << >> fill / over-length / negative / byte-aligned
    "bitstr:sub",      // substring(bit FROM FOR) incl. no-len / neg / overflow
    "bitstr:overlay",  // overlay(bit placing bit) incl. no-len / bounds
    "bitstr:pos",      // position(bit IN bit) empty / longer / offsets
    "bitstr:getset",   // get_bit / set_bit incl. range + new-bit errors
    "bitstr:len",      // length / octet_length / bit_length / bit_count
    "bitstr:cmp",      // comparison operators over unequal-length pairs
    "bitstr:order",    // ORDER BY / DISTINCT / GROUP BY / min/max over VALUES
];

pub fn gen_bitstring_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstring");
    match g.weights.pick(g.rng, SHAPES) {
        "bitstr:io" => gen_io(g),
        "bitstr:typmod" => gen_typmod(g),
        "bitstr:cast" => gen_cast(g),
        "bitstr:cat" => gen_cat(g),
        "bitstr:logic" => gen_logic(g),
        "bitstr:shift" => gen_shift(g),
        "bitstr:sub" => gen_sub(g),
        "bitstr:overlay" => gen_overlay(g),
        "bitstr:pos" => gen_pos(g),
        "bitstr:getset" => gen_getset(g),
        "bitstr:len" => gen_len(g),
        "bitstr:cmp" => gen_cmp(g),
        "bitstr:order" => gen_order(g),
        other => unreachable!("unknown bitstring shape {other}"),
    }
}

fn raw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

fn rawv(stmts: Vec<String>) -> Vec<StmtKind> {
    stmts.into_iter().map(StmtKind::Raw).collect()
}

/// One-knob error-fuel bias: err arms host deliberate matched errors.
fn err_arm(g: &mut Gen) -> bool {
    if g.weights.pick(g.rng, &["bitstr:ok", "bitstr:err"]) == "bitstr:err" {
        g.fire("bitstr:err");
        true
    } else {
        false
    }
}

fn pick<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

/// Deterministic binary bit literal of length `n` with a fixed, width-varied
/// pattern (alternating from the low bit so different lengths differ, and so
/// the last-byte pad bits are exercised for non-multiple-of-8 widths).
fn blit(n: usize) -> String {
    let mut s = String::from("B'");
    for i in 0..n {
        s.push(if i % 2 == 0 { '1' } else { '0' });
    }
    s.push('\'');
    s
}

/// Boundary bit-string widths: empty, 1, sub-byte, byte boundaries and the
/// int-cast edges. TYPEIO's random arms rarely hit these exactly.
const WIDTHS: &[usize] = &[0, 1, 3, 7, 8, 9, 15, 16, 31, 32, 33, 63, 64, 65];

fn width(g: &mut Gen) -> usize {
    *g.rng.pick(WIDTHS)
}

/// bit_in / varbit_in / bits_out: binary + hex spellings, empty string, the
/// upper/lower B/X prefixes, and the invalid-digit error arms (22P02).
fn gen_io(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:io");
    let err = err_arm(g);
    let sql = if err {
        match g.rng.below(3) {
            0 => "SELECT B'1012'::text;".to_string(),        // bad binary digit
            1 => "SELECT X'1G'::text;".to_string(),          // bad hex digit
            _ => "SELECT B'abc'::text;".to_string(),         // letters in binary
        }
    } else {
        match g.rng.below(7) {
            // Empty bit strings, both spellings (bits_in/varbit_in slen==0).
            0 => "SELECT B''::text, X''::text, B''::varbit::text, length(B'');".to_string(),
            // Hex literal round-trip (bits_in hex branch, 4 bits per nibble).
            1 => format!(
                "SELECT X'{h}'::text, x'{h}'::varbit::text;",
                h = pick(g, &["0", "F", "1F", "deadBEEF", "00", "ff", "8"])
            ),
            // Binary literal round-trip at a boundary width.
            2 => {
                let a = blit(1 + width(g).max(1).min(64));
                format!("SELECT {a}::text, {a}::varbit::text;")
            }
            // Upper/lower prefix equivalence + hex vs binary equality.
            3 => "SELECT b'1010'::text, B'1010'::text, x'a'::text, X'A'::text, \
                  (B'1010' = X'A');"
                .to_string(),
            // bits_out over a hex-derived value (odd nibble count pads).
            4 => "SELECT X'ABC'::text, X'ABC'::varbit::text, bit_length(X'ABC');".to_string(),
            // A wide hex literal exercising the multi-byte out loop.
            5 => "SELECT X'0123456789abcdef'::text, length(X'0123456789abcdef');".to_string(),
            // Mixed: cast text through bit and back.
            _ => "SELECT '10110'::bit(5)::text, '1010'::varbit::text, ''::varbit::text;"
                .to_string(),
        }
    };
    raw(sql)
}

/// bit(n) truncate/pad rules (explicit `::` casts pad/truncate silently) and
/// the *assignment-context* errors that the length-coercion functions raise
/// only when `is_explicit` is false: fixed-length mismatch (22026, bit_coerce)
/// and varying-too-long (22001, varbit_coerce). Those need a typed target, so
/// the error arm rides a self-contained TEMP-table bracket (create; erroring
/// INSERT; drop) — no persistent catalog growth, no cross-group state.
fn gen_typmod(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:typmod");
    if err_arm(g) {
        // Assignment coercion into a typed column takes the implicit path.
        let (ty, val) = match g.rng.below(3) {
            // bit(5) column, 3-bit value: "bit string length 3 does not
            // match type bit(5)" (22026).
            0 => ("bit(5)", "B'101'"),
            // bit(4) column, 6-bit value: fixed mismatch the other way.
            1 => ("bit(4)", "B'101010'"),
            // bit varying(3) column, 7-bit value: "bit string too long for
            // type bit varying(3)" (22001).
            _ => ("varbit(3)", "B'1010101'"),
        };
        return rawv(vec![
            "DROP TABLE IF EXISTS fz_bs_tm;".to_string(),
            format!("CREATE TEMP TABLE fz_bs_tm (b {ty});"),
            format!("INSERT INTO fz_bs_tm VALUES ({val});"),
            "DROP TABLE fz_bs_tm;".to_string(),
        ]);
    }
    let sql = match g.rng.below(6) {
        // Explicit bit(n) cast pads with zeros on the right.
        0 => "SELECT B'1'::bit(8)::text, B'11'::bit(4)::text, B''::bit(3)::text;".to_string(),
        // Explicit bit(n) cast truncates (is_explicit path).
        1 => "SELECT B'11111111'::bit(3)::text, X'FF'::bit(4)::text;".to_string(),
        // varbit(n) truncates only when longer; no-op when shorter/equal.
        2 => "SELECT B'101010'::varbit(3)::text, B'10'::varbit(9)::text, \
              B'111'::varbit(3)::text;"
            .to_string(),
        // Typmod display through format_type and the column catalog.
        3 => format!(
            "SELECT format_type('bit'::regtype::oid, {m}), \
             format_type('varbit'::regtype::oid, {m});",
            m = 1 + g.rng.below(64)
        ),
        // Same-length bit(n) cast is a no-op (bit_coerce Ok(None)).
        4 => "SELECT B'10110'::bit(5)::text, B'10110'::varbit(5)::text;".to_string(),
        // bit(1) minimum-width boundary + explicit pad/truncate the short way.
        _ => "SELECT B'0'::bit(1)::text, B'1'::bit(1)::text, B''::varbit(1)::text, \
              B'101'::bit(5)::text;"
            .to_string(),
    };
    raw(sql)
}

/// bit <-> int4 / int8 casts: sign-fill widths, both directions, and the
/// exact overflow boundaries (>32 -> integer 22003, >64 -> bigint 22003).
fn gen_cast(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:cast");
    let err = err_arm(g);
    let sql = if err {
        match g.rng.below(3) {
            // bit(33) -> int4 overflows (bitlen > 32).
            0 => "SELECT B'101010101010101010101010101010101'::int4;".to_string(),
            // bit(65) -> int8 overflows (bitlen > 64).
            1 => "SELECT (0::bit(65))::int8;".to_string(),
            // int -> bit(0) invalid typmod is caught at typmodin (22023);
            // spell it as a table type so the typmod is validated.
            _ => "SELECT 1::bit(0);".to_string(),
        }
    } else {
        match g.rng.below(8) {
            // int4 -> bit(n): positive value across sub/at/over 32-bit widths.
            0 => "SELECT (5::bit(8))::text, (5::bit(4))::text, (5::bit(3))::text, \
                  (5::bit(32))::text;"
                .to_string(),
            // int4 -> bit(n): NEGATIVE value sign-fills the high bits.
            1 => "SELECT ((-1)::bit(8))::text, ((-1)::bit(16))::text, \
                  ((-2)::bit(8))::text, ((-5)::bit(7))::text;"
                .to_string(),
            // int4 -> bit(n) wider than 32 sign-extends into the pad bytes.
            2 => "SELECT ((-1)::bit(40))::text, (1::bit(40))::text, \
                  ((-256)::bit(33))::text;"
                .to_string(),
            // int8 -> bit(64) round-trip and the 64-bit boundary.
            3 => format!(
                "SELECT ({v}::bit(64))::text, ({v}::bigint::bit(64))::int8;",
                v = 1 + g.rng.below(1 << 20)
            ),
            // bit -> int4: MSB-first accumulation, exact width 32.
            4 => "SELECT (B'1'::int4), (B'1000'::int4), \
                  (B'11111111111111111111111111111111'::int4);"
                .to_string(),
            // bit -> int8: exact width 64 and a short value.
            5 => "SELECT (B'1'::bit(8))::bigint, \
                  (B'1111111111111111111111111111111111111111111111111111111111111111'::int8);"
                .to_string(),
            // int8 -> bit(n) narrower than 64 keeps the low bits.
            6 => "SELECT (255::bigint::bit(8))::text, (255::bigint::bit(4))::text, \
                  ((-1)::bigint::bit(9))::text;"
                .to_string(),
            // bit <-> varbit cross-casts (no reinterpretation of bits).
            _ => "SELECT (B'1011'::varbit)::bit(4)::text, (B'1011'::varbit(2))::text, \
                  (X'FF'::bit(8)::varbit)::text;"
                .to_string(),
        }
    };
    raw(sql)
}

/// Concatenation `||`: the aligned fast path (first operand a byte multiple),
/// the *unaligned* path (odd first-operand length exercises bit1pad != 0),
/// and empty operands on either side.
fn gen_cat(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:cat");
    let sql = match g.rng.below(6) {
        // Aligned: 8-bit first operand -> memcpy branch.
        0 => "SELECT (B'10101010' || B'11')::text, (X'FF' || B'0')::text;".to_string(),
        // Unaligned: 3-bit first operand -> bit-shift merge branch.
        1 => "SELECT (B'101' || B'110')::text, (B'1' || B'1111111')::text;".to_string(),
        // Empty on the left / right / both (bitlen2 == 0 short-circuit).
        2 => "SELECT (B'' || B'101')::text, (B'101' || B'')::text, (B'' || B'')::text;"
            .to_string(),
        // Unaligned with a long right operand crossing byte boundaries.
        3 => "SELECT (B'10101' || B'0101010101')::text;".to_string(),
        // Hex + binary mixed concatenation.
        4 => "SELECT (X'A' || B'1')::text, (X'FF' || X'0F')::text;".to_string(),
        // Random-ish widths staying deterministic through blit.
        _ => {
            let a = blit(1 + g.rng.below_usize(7));
            let b = blit(1 + g.rng.below_usize(7));
            format!("SELECT ({a} || {b})::text;")
        }
    };
    raw(sql)
}

/// `& | # ~` bitwise logic, including the different-size error (22026) and
/// the empty-operand identity.
fn gen_logic(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:logic");
    let err = err_arm(g);
    let sql = if err {
        let op = pick(g, &["&", "|", "#"]);
        // Mismatched lengths: "cannot AND/OR/XOR bit strings of different sizes".
        format!("SELECT (B'101' {op} B'10')::text;")
    } else {
        match g.rng.below(5) {
            // All four ops at a common width.
            0 => "SELECT (B'1100' & B'1010')::text, (B'1100' | B'1010')::text, \
                  (B'1100' # B'1010')::text, (~ B'1100')::text;"
                .to_string(),
            // Empty operands (zero-length loop, valid).
            1 => "SELECT (B'' & B'')::text, (B'' | B'')::text, (~ B'')::text;".to_string(),
            // Sub-byte width so pad bits participate then get repadded (~).
            2 => "SELECT (~ B'1')::text, (~ B'101')::text, (~ B'1010101')::text;".to_string(),
            // Multi-byte width crossing a byte boundary.
            3 => "SELECT (B'1111000011110000' & B'1010101010101010')::text, \
                  (B'1111000011110000' # B'1010101010101010')::text;"
                .to_string(),
            // Hex-derived equal-width operands.
            _ => "SELECT (X'FF' & X'0F')::text, (X'FF' | X'0F')::text, \
                  (X'FF' # X'0F')::text;"
                .to_string(),
        }
    };
    raw(sql)
}

/// `<<` / `>>`: zero-fill semantics, shift >= length (all zero), negative
/// shift (reverses direction), and byte-aligned shifts (ishift == 0 path).
fn gen_shift(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:shift");
    let sql = match g.rng.below(6) {
        // Small shifts both directions (bit-merge path, pad_last on >>).
        0 => "SELECT (B'10110' << 2)::text, (B'10110' >> 2)::text, \
              (B'10110' << 1)::text, (B'10110' >> 1)::text;"
            .to_string(),
        // Shift >= length yields all zeros of the same width.
        1 => "SELECT (B'101' << 3)::text, (B'101' >> 3)::text, \
              (B'101' << 64)::text, (B'101' >> 99)::text;"
            .to_string(),
        // Negative shift reverses direction (dispatch into the sibling core).
        2 => "SELECT (B'10110' << -2)::text, (B'10110' >> -2)::text;".to_string(),
        // Byte-aligned shift (multiple of 8) exercises the copy fast path.
        3 => "SELECT (B'1111000010101010' << 8)::text, \
              (B'1111000010101010' >> 8)::text;"
            .to_string(),
        // Shift of the empty and single-bit strings.
        4 => "SELECT (B'' << 3)::text, (B'1' << 1)::text, (B'1' >> 1)::text;".to_string(),
        // Wide value with a mid-byte shift, both directions.
        _ => {
            let s = g.rng.below(20);
            format!(
                "SELECT (B'1011001110001111' << {s})::text, \
                 (B'1011001110001111' >> {s})::text;"
            )
        }
    };
    raw(sql)
}

/// substring(bit FROM FOR): the FOR form, the no-length form, the negative
/// length error (2201B), S+L overflow (runs to end), and out-of-range starts.
fn gen_sub(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:sub");
    let err = err_arm(g);
    let sql = if err {
        // Negative length: "negative substring length not allowed".
        "SELECT substring(B'10110' from 2 for -1)::text;".to_string()
    } else {
        match g.rng.below(6) {
            // FROM/FOR in range.
            0 => "SELECT substring(B'11010111' from 3 for 4)::text, \
                  substring(B'11010111' from 1 for 2)::text;"
                .to_string(),
            // No-length form runs to the end.
            1 => "SELECT substring(B'11010111' from 3)::text, \
                  substring(B'11010111' from 1)::text;"
                .to_string(),
            // Start < 1 clamps to 1; start past end yields empty.
            2 => "SELECT substring(B'11010111' from -2 for 4)::text, \
                  substring(B'11010111' from 20 for 4)::text;"
                .to_string(),
            // FOR 0 yields empty; e1 <= s1 empty path.
            3 => "SELECT substring(B'11010111' from 3 for 0)::text, \
                  substring(B'1010' from 2 for 1)::text;"
                .to_string(),
            // S + L overflow runs to end (checked_add None branch).
            4 => "SELECT substring(B'11010111' from 2 for 2147483647)::text;".to_string(),
            // Substring of the empty string.
            _ => "SELECT substring(B'' from 1 for 3)::text, substring(B'' from 1)::text;"
                .to_string(),
        }
    };
    raw(sql)
}

/// overlay(bit placing bit FROM FOR): the FOR form, the no-length form (sl =
/// placement length), sp <= 0 error (2201B), and sp+sl overflow (22003).
fn gen_overlay(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:overlay");
    let err = err_arm(g);
    let sql = if err {
        match g.rng.below(2) {
            // sp <= 0: "negative substring length not allowed".
            0 => "SELECT overlay(B'10110' placing B'00' from 0)::text;".to_string(),
            // sp + sl overflow: "integer out of range".
            _ => "SELECT overlay(B'10110' placing B'00' from 2147483647 for 2147483647)::text;"
                .to_string(),
        }
    } else {
        match g.rng.below(5) {
            // FROM/FOR replacing an interior run.
            0 => "SELECT overlay(B'10111000' placing B'01' from 3 for 2)::text;".to_string(),
            // No-length form: replacement length = placing length.
            1 => "SELECT overlay(B'10111000' placing B'101' from 3)::text;".to_string(),
            // FOR 0 inserts without deleting.
            2 => "SELECT overlay(B'10111000' placing B'11' from 4 for 0)::text;".to_string(),
            // Overlay at position 1 (empty head prefix).
            3 => "SELECT overlay(B'10111000' placing B'111' from 1 for 2)::text;".to_string(),
            // Overlay past the end extends via catenate.
            _ => "SELECT overlay(B'1010' placing B'111' from 4 for 10)::text;".to_string(),
        }
    };
    raw(sql)
}

/// position(bit IN bit): empty substring (1), empty string (0), substring
/// longer than string (0), matches at various offsets, and no match.
fn gen_pos(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:pos");
    let sql = match g.rng.below(5) {
        // Empty substring returns 1; empty string returns 0.
        0 => "SELECT position(B'' in B'10110'), position(B'1' in B''), \
              position(B'' in B'');"
            .to_string(),
        // Substring longer than string returns 0.
        1 => "SELECT position(B'101010' in B'101'), position(B'11' in B'1');".to_string(),
        // Matches at the start / interior / end.
        2 => "SELECT position(B'101' in B'101101'), position(B'011' in B'101101'), \
              position(B'110' in B'101101');"
            .to_string(),
        // No match despite prefix overlap (mask arithmetic).
        3 => "SELECT position(B'111' in B'101101'), position(B'000' in B'101101');".to_string(),
        // Multi-byte string with a byte-crossing match.
        _ => "SELECT position(B'0101' in B'1111010111110000'), \
              position(B'10000' in B'1111010111110000');"
            .to_string(),
    };
    raw(sql)
}

/// get_bit / set_bit: in-range at the 0 and len-1 boundaries, the subscript
/// out-of-range error (2202E), and the new-bit not 0/1 error (22023).
fn gen_getset(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:getset");
    let err = err_arm(g);
    let sql = if err {
        match g.rng.below(3) {
            // Index >= length: "bit index N out of valid range".
            0 => "SELECT get_bit(B'101', 3);".to_string(),
            // Negative index.
            1 => "SELECT set_bit(B'101', -1, 1)::text;".to_string(),
            // new bit not 0 or 1: "new bit must be 0 or 1".
            _ => "SELECT set_bit(B'101', 1, 2)::text;".to_string(),
        }
    } else {
        match g.rng.below(4) {
            // Boundary indices 0 and len-1.
            0 => "SELECT get_bit(B'101101', 0), get_bit(B'101101', 5), \
                  set_bit(B'101101', 0, 0)::text, set_bit(B'101101', 5, 1)::text;"
                .to_string(),
            // Interior get/set, clearing and setting.
            1 => "SELECT get_bit(B'11110000', 3), set_bit(B'11110000', 3, 0)::text, \
                  set_bit(B'11110000', 4, 1)::text;"
                .to_string(),
            // Multi-byte value: index crossing a byte boundary.
            2 => "SELECT get_bit(B'1111000010101010', 8), get_bit(B'1111000010101010', 9), \
                  set_bit(B'1111000010101010', 8, 1)::text;"
                .to_string(),
            // Single-bit string boundary.
            _ => "SELECT get_bit(B'1', 0), set_bit(B'1', 0, 0)::text, \
                  get_bit(B'0', 0);"
                .to_string(),
        }
    };
    raw(sql)
}

/// length / octet_length / bit_length / bit_count over boundary widths,
/// including the empty string.
fn gen_len(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:len");
    let sql = match g.rng.below(4) {
        0 => "SELECT length(B''), octet_length(B''), bit_length(B''), bit_count(B'');"
            .to_string(),
        1 => "SELECT length(B'1'), octet_length(B'1'), bit_length(B'1'), bit_count(B'1');"
            .to_string(),
        2 => "SELECT length(B'101010101'), octet_length(B'101010101'), \
              bit_length(B'101010101'), bit_count(B'101010101');"
            .to_string(),
        _ => "SELECT length(X'FF00'), octet_length(X'FF00'), bit_length(X'FF00'), \
              bit_count(X'FF00'), bit_count(B'11111111');"
            .to_string(),
    };
    raw(sql)
}

/// Comparison operators over unequal-length pairs (bit_cmp_payload: shorter
/// prefix-equal string sorts first) and the full operator matrix.
fn gen_cmp(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:cmp");
    let sql = match g.rng.below(4) {
        // Full operator matrix at equal length.
        0 => "SELECT B'101' = B'101', B'101' <> B'110', B'101' < B'110', \
              B'101' <= B'101', B'110' > B'101', B'101' >= B'101';"
            .to_string(),
        // Unequal length, common prefix: shorter sorts less.
        1 => "SELECT B'10' < B'101', B'101' < B'1010', B'10' = B'100', \
              B'1' < B'10';"
            .to_string(),
        // Unequal length, differing prefix byte dominates the length.
        2 => "SELECT B'110' < B'1011', B'0111' < B'10', B'' < B'0', B'' = B'';".to_string(),
        // Hex vs binary equality across spellings.
        _ => "SELECT X'F' = B'1111', X'A0' = B'10100000', X'0' < X'1';".to_string(),
    };
    raw(sql)
}

/// ORDER BY / DISTINCT / GROUP BY / min / max / bit_and|or|xor aggregates
/// over an inline VALUES list (comparison + hashing, no persistent table).
/// Deterministic value pool with a total `ORDER BY … ::text`.
fn gen_order(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("bitstr:order");
    // A fixed, mixed-width value pool (includes the empty bit string and a
    // duplicate so DISTINCT / GROUP BY have something to fold).
    let pool = "(B'101'), (B'01'), (B'101'), (B''), (B'1100'), (B'0'), (B'1'), (B'11')";
    let sql = match g.rng.below(5) {
        // Total ORDER BY over the raw values (ordered-compare, ::text).
        0 => format!(
            "SELECT v::text FROM (VALUES {pool}) t(v) ORDER BY v, v::text;"
        ),
        // DISTINCT folds the duplicate (hashing), ordered for compare.
        1 => format!(
            "SELECT DISTINCT v::text FROM (VALUES {pool}) t(v) ORDER BY v::text;"
        ),
        // GROUP BY + count (hash aggregate over bit), ordered for compare.
        2 => format!(
            "SELECT v::text, count(*) FROM (VALUES {pool}) t(v) \
             GROUP BY v ORDER BY v::text;"
        ),
        // min / max via bitcmp.
        3 => format!("SELECT min(v)::text, max(v)::text FROM (VALUES {pool}) t(v);"),
        // Equal-width aggregate ops (bit_and/or/xor need matching lengths).
        _ => "SELECT bit_and(v)::text, bit_or(v)::text, bit_xor(v)::text \
              FROM (VALUES (B'1100'), (B'1010'), (B'0110')) t(v);"
            .to_string(),
    };
    raw(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Generate `n` groups and collect (statements, fired productions).
    fn gen_groups(seed: u64, n: usize) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_bitstring_module(&mut g);
            assert!(!stmts.is_empty());
            groups.push(stmts.iter().map(|s| s.to_sql()).collect::<Vec<_>>());
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    #[test]
    fn shapes_and_textual_invariants() {
        let (groups, prods) = gen_groups(0xB175, 5000);
        for group in &groups {
            for sql in group {
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.starts_with("SELECT ") || sql.starts_with("DROP ")
                    || sql.starts_with("CREATE ") || sql.starts_with("INSERT "), "{sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                for banned in ["random(", "now()", "gen_random_uuid", "clock_timestamp"] {
                    assert!(!sql.contains(banned), "volatile fn in {sql}");
                }
            }
        }
        // Every top-level shape and the ok/err bias fired at least once.
        for shape in SHAPES {
            assert!(prods.iter().any(|p| p == shape), "shape never fired: {shape}");
        }
        assert!(prods.iter().any(|p| p == "bitstring"));
        assert!(prods.iter().any(|p| p == "bitstr:err"), "error arm never fired");
    }

    /// The TEMP-table error bracket always creates and drops its fixture in
    /// the same group (no persistent catalog growth, name-collision safe).
    #[test]
    fn typmod_error_bracket_is_self_contained() {
        let (groups, _) = gen_groups(0xB176, 5000);
        for group in &groups {
            let creates = group.iter().filter(|s| s.starts_with("CREATE TEMP TABLE")).count();
            if creates > 0 {
                assert_eq!(creates, 1, "more than one temp table per group: {group:?}");
                assert!(group.iter().any(|s| s == "DROP TABLE fz_bs_tm;"),
                    "temp table not dropped: {group:?}");
            }
        }
    }
}
