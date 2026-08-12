//! Q2 mbconv module: the multibyte encoding-conversion production
//! (sql-reachable-queue chunk `mbconv-sweep`, 251 fns — utf8_and_*,
//! *_to_* conversion_procs, pg_verify_mbstr/pg_mblen dispatch, plus the
//! report_invalid_encoding / report_untranslatable_char error paths).
//!
//! One production, driven entirely by verified data tables
//! (`crate::mbconv_data` — GENERATED from the pinned REL_18_3 reference
//! and pgrust origin/main@9dd02888bf7; both engines' pg_conversion
//! matrices were diffed byte-identical (128 pairs) and every emitted
//! (pair x payload) cell was executed on BOTH engines with matching
//! output before being baked in). Statement shapes:
//!
//!   mbconv:conv    convert(bytea, src, dst) over the verified cell table
//!                  (ok cells convert; err cells raise the matched 22P05
//!                  "has no equivalent" — weighted via mbconv:ok/err)
//!   mbconv:from    convert_from(bytea, src) — src -> database encoding
//!                  (UTF8; every source has a *_to_utf8 conversion)
//!   mbconv:to      convert_to(text, dst) over the verified (enc, text)
//!                  representability table
//!   mbconv:len     length(bytea, enc) direct (OID 1713) + bit/octet_length probes
//!                  (pg_verify_mbstr + pg_mblen sweep per encoding)
//!   mbconv:bad     invalid-byte-sequence fuel (matched 22021), low
//!                  weight via the mbconv:ok/mbconv:err knob
//!
//! Determinism: every payload is a fixed hex literal; convert() output is
//! bytea (::text hex form, byte-stable); convert_from output is UTF8 text
//! from a verified-valid source sequence. No engine-computed anything.

use crate::mbconv_data::{BAD_SEQS, CONV_CELLS, ENCODINGS, ENC_TEXTS};
use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "mbconv:conv",
    "mbconv:from",
    "mbconv:to",
    "mbconv:len",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_mbconv_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("mbconv");
    let sql = match g.weights.pick(g.rng, SHAPES) {
        "mbconv:conv" => gen_conv(g),
        "mbconv:from" => gen_from(g),
        "mbconv:to" => gen_to(g),
        "mbconv:len" => gen_len(g),
        other => unreachable!("unknown mbconv shape {other}"),
    };
    vec![StmtKind::Raw(sql)]
}

/// Error-fuel knob: err arms pick 22P05/22021-verified cells.
fn err_arm(g: &mut Gen) -> bool {
    if g.weights.pick(g.rng, &["mbconv:ok", "mbconv:err"]) == "mbconv:err" {
        g.fire("mbconv:err");
        true
    } else {
        false
    }
}

/// A verified convert() cell with the requested verdict; the tables carry
/// both, so a matching cell always exists.
fn pick_cell(g: &mut Gen, want_ok: bool) -> &'static crate::mbconv_data::ConvCell {
    loop {
        let c = &CONV_CELLS[g.rng.below_usize(CONV_CELLS.len())];
        if c.ok == want_ok {
            return c;
        }
    }
}

fn gen_conv(g: &mut Gen) -> String {
    g.fire("mbconv:conv");
    let err = err_arm(g);
    let c = pick_cell(g, !err);
    // Alternate the output spelling: raw bytea text (hex) or encode().
    if g.rng.chance(1, 3) {
        format!(
            "SELECT encode(convert('\\x{}'::bytea, '{}', '{}'), 'hex');",
            c.hex, c.src, c.dst
        )
    } else {
        format!(
            "SELECT convert('\\x{}'::bytea, '{}', '{}')::text;",
            c.hex, c.src, c.dst
        )
    }
}

fn gen_from(g: &mut Gen) -> String {
    g.fire("mbconv:from");
    if err_arm(g) {
        // Matched 22021: invalid byte sequence for the named encoding.
        let b = &BAD_SEQS[g.rng.below_usize(BAD_SEQS.len())];
        return format!("SELECT convert_from('\\x{}'::bytea, '{}');", b.hex, b.src);
    }
    // Every source-side pool payload converts to UTF8 (the db encoding):
    // the *_to_utf8 cells in the table are all ok.
    let c = pick_cell(g, true);
    if c.dst == "UTF8" || g.rng.chance(1, 2) {
        format!("SELECT convert_from('\\x{}'::bytea, '{}');", c.hex, c.src)
    } else {
        // Round-trip spelling: src -> dst plus octet_length of the
        // converted bytes (both verified cells).
        format!(
            "SELECT convert_from('\\x{}'::bytea, '{}'), octet_length(convert('\\x{}'::bytea, '{}', '{}'));",
            c.hex, c.src, c.hex, c.src, c.dst
        )
    }
}

fn gen_to(g: &mut Gen) -> String {
    g.fire("mbconv:to");
    if err_arm(g) {
        // Untranslatable character: cyrillic into a latin target (verified
        // matched 22P05 on both engines via the cell table's err rows).
        let c = pick_cell(g, false);
        if c.src == "UTF8" {
            // A utf8 err cell IS a convert_to failure: materialize the
            // exact text from its verified payload, then hit the same
            // failing conversion through the convert_to spelling.
            return format!(
                "SELECT convert_to(convert_from('\\x{}'::bytea, 'UTF8'), '{}');",
                c.hex, c.dst
            );
        }
        return format!(
            "SELECT convert('\\x{}'::bytea, '{}', '{}');",
            c.hex, c.src, c.dst
        );
    }
    let et = &ENC_TEXTS[g.rng.below_usize(ENC_TEXTS.len())];
    if g.rng.chance(1, 3) {
        format!(
            "SELECT encode(convert_to('{}', '{}'), 'hex');",
            et.text, et.enc
        )
    } else {
        format!("SELECT convert_to('{}', '{}')::text;", et.text, et.enc)
    }
}

/// Length/verify probes. Q2-F1 is FIXED on main (fc_length_in_encoding,
/// OID 1713 — PR #814), so the direct length(bytea, name) spelling is
/// armed: it drives pg_verify_mbstr_len on the B side; the err arm hits
/// the matched 22021 invalid-byte-sequence path.
fn gen_len(g: &mut Gen) -> String {
    g.fire("mbconv:len");
    if err_arm(g) {
        let b = &BAD_SEQS[g.rng.below_usize(BAD_SEQS.len())];
        // Direct spelling on the error path: report_invalid_encoding
        // (22021) is shared with convert_from — matched both sides.
        return format!("SELECT length('\\x{}'::bytea, '{}');", b.hex, b.src);
    }
    let c = pick_cell(g, true);
    match g.rng.below(3) {
        0 => format!(
            "SELECT length('\\x{}'::bytea, '{}');",
            c.hex, c.src
        ),
        1 => format!(
            "SELECT length('\\x{}'::bytea, '{}'), octet_length('\\x{}'::bytea), bit_length('\\x{}'::bytea);",
            c.hex, c.src, c.hex, c.hex
        ),
        _ => {
            // Encoding-name round trip through the id space keeps the
            // pg_char_to_encoding/pg_encoding_to_char pair hot.
            let e = ENCODINGS[g.rng.below_usize(ENCODINGS.len())];
            format!(
                "SELECT pg_char_to_encoding('{}') >= 0, pg_encoding_to_char(pg_char_to_encoding('{}')), length('\\x{}'::bytea, '{}');",
                e, e, c.hex, c.src
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every shape + the err arm fires; statements are single-line,
    /// terminated, balanced, and reference only table-verified payloads.
    #[test]
    fn mbconv_shapes_fire_and_hold_invariants() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x2B01);
        let mut prods_all = Vec::new();
        for _ in 0..3000 {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_mbconv_module(&mut g);
            assert_eq!(stmts.len(), 1);
            let sql = stmts[0].to_sql();
            assert!(sql.ends_with(';'), "{sql}");
            assert!(!sql.contains('\n'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            prods_all.extend(prods);
        }
        for p in SHAPES.iter().chain(&["mbconv:err"]) {
            assert!(prods_all.iter().any(|q| q == p), "{p} never fired");
        }
    }

    /// Same seed -> byte-identical statements.
    #[test]
    fn mbconv_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(77);
            let mut out = Vec::new();
            for _ in 0..300 {
                let mut prods = Vec::new();
                let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                out.push(gen_mbconv_module(&mut g)[0].to_sql());
            }
            out
        };
        assert_eq!(run(), run());
    }

    /// The data tables are internally consistent: both verdicts present,
    /// every BAD_SEQS src is a known encoding, hex payloads are hex.
    #[test]
    fn mbconv_data_tables_are_consistent() {
        assert!(CONV_CELLS.iter().any(|c| c.ok));
        assert!(CONV_CELLS.iter().any(|c| !c.ok));
        for c in CONV_CELLS {
            assert!(c.hex.chars().all(|ch| ch.is_ascii_hexdigit()), "{}", c.hex);
            assert!(ENCODINGS.contains(&c.src), "{}", c.src);
        }
        for b in BAD_SEQS {
            assert!(ENCODINGS.contains(&b.src), "{}", b.src);
        }
        for t in ENC_TEXTS {
            assert!(!t.text.contains('\'') && !t.text.contains('\\'), "{}", t.text);
        }
    }
}
